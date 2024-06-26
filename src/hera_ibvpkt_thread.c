/* hera_ibvpkt_thread.c
 *
 * Routine to process packets from data buffer populated by ibvpkt_thread.
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <errno.h>

#include <xgpu.h>

#include "hashpipe.h"
#include "hashpipe_ibvpkt_databuf.h"
#include "paper_databuf.h"

//#include <endian.h>
#include "hashpipe_packet.h"
#include "nt_memutils.h"

// The HERA F-Engine packets arrive over the wire with this layout (first 64
// bytes shown):
//
//     0x0000:  0202 0a50 28c8 0033 eba3 1701 0800 4500
//     0x0010:  1224 0000 4000 ff11 040c 0a50 2855 0a50
//     0x0020:  28c8 2142 213f 1210 0000 0226 a140 8f00
//     0x0030:  0057 7d7e 7171 3131 d9d9 e4d4 2222 3737
//     0x0040:  2717 9191 c4c5 2f2f 0f1f 6969 7979 a191
//
// This consists of a 42 byte network (eth/ip/udp) header, an 8 byte payload
// header, and 4608 bytes of payload data.
//
// By using IBVPKTSZ=42,8,4608, each of these regions is padded to the next
// multiple of 64, so this packet gets stored in memory like this:
//
//     0x0000:  0202 0a50 28c8 0033 eba3 1701 0800 4500
//     0x0010:  1224 0000 4000 ff11 040c 0a50 2855 0a50
//     0x0020:  28c8 2142 213f 1210 0000 ---- ---- ----
//     0x0030:  ---- ---- ---- ---- ---- ---- ---- ----
//     0x0040:  0226 a140 8f00 0057 ---- ---- ---- ---- 
//     0x0050:  ---- ---- ---- ---- ---- ---- ---- ----
//     0x0060:  ---- ---- ---- ---- ---- ---- ---- ----
//     0x0070:  ---- ---- ---- ---- ---- ---- ---- ----
//     0x0080:  7d7e 7171 3131 d9d9 e4d4 2222 3737 2717
//     0x0090:  9191 c4c5 2f2f 0f1f 6969 7979 a191 ....

// HERA F-Engine packet with link layer header and internal padding to optimize
// alignment.  The alignment is acheived through judicious use of IB Verbs
// scatter/gather capabilities (specifically the scatter part).
struct __attribute__ ((__packed__)) hera_ibv_feng_pkt {
  struct ethhdr ethhdr;
  struct iphdr iphdr;
  struct udphdr udphdr;
  uint8_t pad0[22];
  uint64_t hdr;
  uint8_t pad1[56];
  uint64_t payload[576];
};

// HERA F-Engine header byte offset within (unpadded) packet
#define PKT_OFFSET_HERA_FENG_HEADER \
  (sizeof(struct ethhdr) + \
   sizeof(struct iphdr ) + \
   sizeof(struct udphdr))

// HERA F-Engine payload byte offset within (unpadded) packet
#define PKT_OFFSET_HERA_FENG_PAYLOAD \
  (PKT_OFFSET_HERA_FENG_HEADER + sizeof(uint64_t))


#define DEBUG_NET

#ifndef MIN
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

#ifndef MAX
#define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif

//#define PKTSOCK_BYTES_PER_FRAME (16384)
//#define PKTSOCK_FRAMES_PER_BLOCK (8)
//#define PKTSOCK_NBLOCKS (800)
//#define PKTSOCK_NFRAMES (PKTSOCK_FRAMES_PER_BLOCK * PKTSOCK_NBLOCKS)
#define PKTSOCK_BYTES_PER_FRAME (4864)
#define PKTSOCK_FRAMES_PER_BLOCK (128)
#define PKTSOCK_NBLOCKS (5000)
#define PKTSOCK_NFRAMES (PKTSOCK_FRAMES_PER_BLOCK * PKTSOCK_NBLOCKS)

typedef struct {
    uint64_t mcnt;      // m-index of block in output buffer (runs from 0 to Nm)
    uint64_t time;      // First time sample in a packet
    int      chan;      // First channel in a packet
    int      ant;       // Antenna in a packet
} packet_header_t;

// The fields of a block_info_t structure hold (at least) two different kinds
// of data.  Some fields hold data that persist over many packets while other
// fields hold data that are only applicable to the current packet (or the
// previous packet).
typedef struct {
    int initialized;
    int32_t  self_xid;
    uint64_t mcnt_start;
    uint64_t mcnt_log_late;
    int out_of_seq_cnt;
    int block_i;
    int m; // m-index of block in output buffer (runs from 0 to Nm)
    int t; // first time sample in the packet // formerly known as sub_block_i
    int c; // first channel in the packet
    int a; // antenna in the packet
    int block_packet_counter[N_INPUT_BLOCKS];
} block_info_t;

static hashpipe_status_t *st_p;

// A variable to store whether this receiver is processing
// even or odd samples (or in principle a subset of a higher
// order 2^<integer> split
static int time_index;

// Variables used to throttle bursty messages
static int burst_message_counter = 0;
static int burst_message_threshold =  120;
static int burst_max_duration_secs = 3600;
static time_t burst_start = 0;

#if 0
static void print_pkt_header(packet_header_t * pkt_header) {

    static long int prior_mcnt;

    printf("packet header : mcnt %012lx (diff from prior %lld) fid %d xid %d\n",
           pkt_header->mcnt, pkt_header->mcnt-prior_mcnt, pkt_header->fid, pkt_header->xid);

    prior_mcnt = pkt_header->mcnt;
}
#endif

#ifdef DIE_ON_OUT_OF_SEQ_FILL
static void print_block_info(block_info_t * binfo) {
    printf("binfo : mcnt_start %012lx block_i %d t=%02d c=%d a=%d\n",
           binfo->mcnt_start, binfo->block_i, binfo->t, binfo->c, binfo->a);
}

static void print_block_packet_counter(block_info_t * binfo) {
    int i;
    for(i=0;i<N_INPUT_BLOCKS;i++) {
        if(i == binfo->block_i) {
                fprintf(stdout, "*%03d ", binfo->block_packet_counter[i]);      
        } else {
                fprintf(stdout, " %03d ", binfo->block_packet_counter[i]);      
        }
    }
    fprintf(stdout, "\n");
}

static void print_ring_mcnts(paper_input_databuf_t *paper_input_databuf_p) {

    int i;

    for(i=0; i < N_INPUT_BLOCKS; i++) {
        printf("block %d mcnt %012lx\n", i, paper_input_databuf_p->block[i].header.mcnt);
    }
}
#endif // DIE_ON_OUT_OF_SEQ_FILL

// Returns physical block number for given mcnt
static inline int block_for_mcnt(uint64_t mcnt)
{
    return ((mcnt / TIME_DEMUX) / N_TIME_PER_BLOCK) % N_INPUT_BLOCKS;
}

// Returns start mcnt for the block containing a  given mcnt
static inline uint64_t start_for_mcnt(uint64_t mcnt)
{
    uint64_t mcnt_time_index = ((mcnt / N_TIME_PER_PACKET) % TIME_DEMUX);
    return (mcnt - (mcnt%(N_TIME_PER_BLOCK * TIME_DEMUX)) + (mcnt_time_index*N_TIME_PER_PACKET));
}

#ifdef LOG_MCNTS
#define MAX_MCNT_LOG (1024*1024)
//static uint64_t mcnt_log[MAX_MCNT_LOG];
//static int mcnt_log_idx = 0;
static int total_packets_counted = 0;
static int expected_packets_counted = 0;
static int late_packets_counted = 0;
static int outofseq_packets_counted = 0;
static int filled_packets_counted = 0;

void dump_mcnt_log(int xid)
{
    //int i;
    char fname[80];
    FILE *f;
    sprintf(fname, "mcnt.xid%02d.log", xid);
    f = fopen(fname,"w");
    fprintf(f, "expected packets counted = %d\n", expected_packets_counted);
    fprintf(f, "late     packets counted = %d\n", late_packets_counted);
    fprintf(f, "outofseq packets counted = %d\n", outofseq_packets_counted);
    fprintf(f, "total    packets counted = %d\n", total_packets_counted);
    fprintf(f, "filled   packets counted = %d\n", filled_packets_counted);
    //for(i=0; i<MAX_MCNT_LOG; i++) {
    //    if(mcnt_log[i] == 0) break;
    //    fprintf(f, "%012lx\n", mcnt_log[i]);
    //}
    fclose(f);
}
#endif

static inline void get_header (unsigned char *p_frame, packet_header_t * pkt_header)
{
#ifdef TIMING_TEST
    static int pkt_counter=0;
    // HERA TODO
    //pkt_header->mcnt = (pkt_counter / (Nx*Nq*Nf)) %  Nm;
    //pkt_header->xid  = (pkt_counter / (   Nq*Nf)) %  Nx;
    //pkt_header->fid  = (pkt_counter             ) % (Nq*Nf);
    //pkt_counter++;
#else
    uint64_t raw_header;
    raw_header = be64toh(((struct hera_ibv_feng_pkt *)p_frame)->hdr);
    // raw header contains value of first time sample, not mcnt, as defined in this code
    //pkt_header->time        = (raw_header >> 27) & ((1L<<37)-1);
    //pkt_header->mcnt        = pkt_header->time >> 5;
    //pkt_header->mcnt        = (raw_header >> 32) & 0xffffffff;
    pkt_header->mcnt        = (raw_header >> 29) & ((1L<<35)-1);
    pkt_header->chan        = (raw_header >> 16) & ((1<<13)-1);
    pkt_header->ant         =  raw_header        & ((1<<16)-1);
#endif

#ifdef LOG_MCNTS
    total_packets_counted++;
    //mcnt_log[mcnt_log_idx++] = pkt_header->mcnt;
    //if(mcnt_log_idx == MAX_MCNT_LOG) {
    //    dump_mcnt_log(pkt_header->xid);
    //    abort();
    //}
    // HERA TODO
    if(total_packets_counted == 10*1000*1000) {
        dump_mcnt_log(pkt_header->chan);
        abort();
    }
#endif
}

#ifdef DIE_ON_OUT_OF_SEQ_FILL
static void die(paper_input_databuf_t *paper_input_databuf_p, block_info_t *binfo)
{
    print_block_info(binfo);
    print_block_packet_counter(binfo);
    print_ring_mcnts(paper_input_databuf_p);
#ifdef LOG_MCNTS
    dump_mcnt_log();
#endif
    abort(); // End process and generate core file (if ulimit allows)
}
#endif

// This sets the "current" block to be marked as filled.  The current block is
// the block corresponding to binfo->mcnt_start.  Returns mcnt of the block
// being marked filled.
static uint64_t set_block_filled(paper_input_databuf_t *paper_input_databuf_p, block_info_t *binfo)
{
    static int last_filled = -1;

    uint32_t block_missed_pkt_cnt=N_PACKETS_PER_BLOCK, block_missed_mod_cnt, block_missed_feng, missed_pkt_cnt=0;

    uint32_t block_i = block_for_mcnt(binfo->mcnt_start);

    // Validate that we're filling blocks in the proper sequence
    last_filled = (last_filled+1) % N_INPUT_BLOCKS;
    if(last_filled != block_i) {
        printf("block %d being marked filled, but expected block %d!\n", block_i, last_filled);

#ifdef DIE_ON_OUT_OF_SEQ_FILL
        die(paper_input_databuf_p, binfo);
#endif
    }

    // Validate that block_i matches binfo->block_i
    if(block_i != binfo->block_i) {
        hashpipe_warn(__FUNCTION__,
                "block_i for binfo's mcnt (%d) != binfo's block_i (%d)",
                block_i, binfo->block_i);
    }
#ifdef LOG_MCNTS
    filled_packets_counted += binfo->block_packet_counter[block_i];
#endif

    // If all packets are accounted for, mark this block as good
    if(binfo->block_packet_counter[block_i] == N_PACKETS_PER_BLOCK) {
        paper_input_databuf_p->block[block_i].header.good_data = 1;
    }

    // Set the block as filled
    if(paper_input_databuf_set_filled(paper_input_databuf_p, block_i) != HASHPIPE_OK) {
        hashpipe_error(__FUNCTION__, "error waiting for databuf filled call");
        pthread_exit(NULL);
    }

    // Calculate missing packets.
    block_missed_pkt_cnt = N_PACKETS_PER_BLOCK - binfo->block_packet_counter[block_i];
    //fprintf(stderr, "Packets in block %d: %d, N_PACKETS_PER_BLOCK: %d, N_PACKETS_PER_BLOCK_PER_F: %d,  mod_cnt: %d\n", block_i, binfo->block_packet_counter[block_i], N_PACKETS_PER_BLOCK,  N_PACKETS_PER_BLOCK_PER_F, block_missed_pkt_cnt % N_PACKETS_PER_BLOCK_PER_F);
    // If we missed more than N_PACKETS_PER_BLOCK_PER_F, then assume we
    // are missing one or more F engines.  Any missed packets beyond an
    // integer multiple of N_PACKETS_PER_BLOCK_PER_F will be considered
    // as dropped packets.
    block_missed_feng    = N_INPUTS_PER_PACKET / 2 * block_missed_pkt_cnt / N_PACKETS_PER_BLOCK_PER_F;
    block_missed_mod_cnt = block_missed_pkt_cnt % N_PACKETS_PER_BLOCK_PER_F;

    // Reinitialize our XID to -1 (unknown until read from status buffer)
    binfo->self_xid = -1;

    // Update status buffer
    hashpipe_status_lock_busywait_safe(st_p);
    hputu4(st_p->buf, "NETBKOUT", block_i);
    hputu4(st_p->buf, "MISSEDFE", block_missed_feng);
    if(block_missed_mod_cnt) {
        //fprintf(stdout, "Expected %d packets, Got %d\n", N_PACKETS_PER_BLOCK, binfo->block_packet_counter[block_i]);
        // Increment MISSEDPK by number of missed packets for this block
        hgetu4(st_p->buf, "MISSEDPK", &missed_pkt_cnt);
        missed_pkt_cnt += block_missed_mod_cnt;
        hputu4(st_p->buf, "MISSEDPK", missed_pkt_cnt);
    //  fprintf(stderr, "got %d packets instead of %d\n",
    //      binfo->block_packet_counter[block_i], N_PACKETS_PER_BLOCK);
    }
    // Update our XID from status buffer
    hgeti4(st_p->buf, "XID", &binfo->self_xid);
    hashpipe_status_unlock_safe(st_p);

    return binfo->mcnt_start;
}

static inline int calc_block_indexes(block_info_t *binfo, packet_header_t * pkt_header)
{
    if(pkt_header->ant >= Na) {
        hashpipe_error(__FUNCTION__,
                "current packet Antenna ID %u out of range (0-%d)",
                pkt_header->ant, Na-1);
        return -1;
// HERA TODO
//    } else if(pkt_header->chan != binfo->self_xid && binfo->self_xid != -1) {
//      hashpipe_error(__FUNCTION__,
//              "unexpected packet XID %d (expected %d)",
//              pkt_header->xid, binfo->self_xid);
//      return -1;
    }

    //binfo->t = pkt_header->time;
    binfo->m = ((pkt_header->mcnt/TIME_DEMUX) % N_TIME_PER_BLOCK) / N_TIME_PER_PACKET;
    binfo->a = pkt_header->ant;
    binfo->c = pkt_header->chan % Nc;

    return 0;
}

// This allows for 2 out of sequence packets from each F engine (in a row)
#define MAX_OUT_OF_SEQ (2*Na)

// This allows packets to be two full databufs late without being considered
// out of sequence.
#define LATE_PKT_MCNT_THRESHOLD (2*TIME_DEMUX*N_TIME_PER_BLOCK*N_INPUT_BLOCKS)

// Initialize a block by clearing its "good data" flag and saving the first
// (i.e. earliest) mcnt of the block.  Note that mcnt does not have to be a
// multiple of Nm (number of mcnts per block).  In theory, the block's data
// could be cleared as well, but that takes time and is largely unnecessary in
// a properly functionong system.
static inline void initialize_block(paper_input_databuf_t * paper_input_databuf_p, uint64_t mcnt)
{
    int block_i = block_for_mcnt(mcnt);
    uint64_t mcnt_time_index;

    paper_input_databuf_p->block[block_i].header.good_data = 0;
    // Round pkt_mcnt down to nearest multiple of N_TIME_PER_BLOCK
    mcnt_time_index = ((mcnt / N_TIME_PER_PACKET) % TIME_DEMUX);
    if (mcnt_time_index != time_index) {
        fprintf(stderr, "Expected packets from time index %d, but got index %lu\n", time_index, mcnt_time_index);
    }
    paper_input_databuf_p->block[block_i].header.mcnt = start_for_mcnt(mcnt);
}

// This function must be called once and only once per block_info structure!
// Subsequent calls are no-ops.
static inline void initialize_block_info(block_info_t * binfo)
{
    int i;

    // If this block_info structure has already been initialized
    if(binfo->initialized) {
        return;
    }

    for(i = 0; i < N_INPUT_BLOCKS; i++) {
        binfo->block_packet_counter[i] = 0;
    }

    // Initialize our XID to -1 (unknown until read from status buffer)
    binfo->self_xid = -1;
    // Update our XID from status buffer
    hashpipe_status_lock_busywait_safe(st_p);
    hgeti4(st_p->buf, "XID", &binfo->self_xid);
    hashpipe_status_unlock_safe(st_p);

    // On startup mcnt_start will be zero and mcnt_log_late will be Nm.
    binfo->mcnt_start = time_index;
    binfo->mcnt_log_late = N_TIME_PER_BLOCK*TIME_DEMUX + time_index;
    binfo->block_i = 0;

    binfo->out_of_seq_cnt = 0;
    binfo->initialized = 1;
}

// This function returns -1 unless the given packet causes a block to be marked
// as filled in which case this function returns the marked block's first mcnt.
// Any return value other than -1 will be stored in the status memory as
// NETMCNT, so it is important that values other than -1 are returned rarely
// (i.e. when marking a block as filled)!!!
static inline uint64_t process_packet(
        paper_input_databuf_t *paper_input_databuf_p, unsigned char*p_frame)
{

    static block_info_t binfo;
    packet_header_t pkt_header;
    const uint64_t *payload_p;
    int pkt_block_i;
    int i;
    uint64_t *dest_p;
    int64_t pkt_mcnt_dist;
    uint64_t pkt_mcnt;
    uint64_t cur_mcnt;
    uint64_t netmcnt = -1; // Value to return (!=-1 is stored in status memory)
    //int i;
#if N_DEBUG_INPUT_BLOCKS == 1
    static uint64_t debug_remaining = -1ULL;
    static off_t debug_offset = 0;
    uint64_t * debug_ptr;
#endif

    // Lazy init binfo
    if(!binfo.initialized) {
        initialize_block_info(&binfo);
    }

    // Parse packet header
    get_header(p_frame, &pkt_header);
    // mcnt is a spectra count, representing the first
    // time sample in the packet
    pkt_mcnt = pkt_header.mcnt;
    pkt_block_i = block_for_mcnt(pkt_mcnt);
    cur_mcnt = binfo.mcnt_start;

    // Packet mcnt distance (how far away is this packet's mcnt from the
    // current mcnt).  Positive distance for pcnt mcnts > current mcnt.
    pkt_mcnt_dist = pkt_mcnt - cur_mcnt;

#if N_DEBUG_INPUT_BLOCKS == 1
    debug_ptr = (uint64_t *)&paper_input_databuf_p->block[N_INPUT_BLOCKS];
    debug_ptr[debug_offset++] = be64toh(((struct hera_ibv_feng_pkt *)p_frame)->hdr);
    if(--debug_remaining == 0) {
        exit(1);
    }
    if(debug_offset >= sizeof(paper_input_block_t)/sizeof(uint64_t)) {
        debug_offset = 0;
    }
#endif

    //fprintf(stdout, "mcnt:%lu, time:%lu, ant:%d, chan:%d\n", pkt_header.mcnt,
    //    pkt_header.time, pkt_header.ant, pkt_header.chan);
    //if(pkt_header.ant==0){
    //    fprintf(stdout, "mcnt:%lu, time:%lu, ant:%d, chan:%d, block:%d \n", pkt_header.mcnt,
    //    pkt_header.time, pkt_header.ant, pkt_header.chan, pkt_block_i);
    //}
    //if(pkt_header.ant==69){
    //    fprintf(stdout, "mcnt:%lu, time:%lu, ant:%d, chan:%d\n", pkt_header.mcnt,
    //    pkt_header.time, pkt_header.ant, pkt_header.chan);
    //}
    //if(pkt_header.ant==138){
    //    fprintf(stdout, "mcnt:%lu, time:%lu, ant:%d, chan:%d\n", pkt_header.mcnt,
    //    pkt_header.time, pkt_header.ant, pkt_header.chan);
    //}

    // We expect packets for the current block, the next block, and the block after.
    if(0 <= pkt_mcnt_dist && pkt_mcnt_dist < 3*N_TIME_PER_BLOCK*TIME_DEMUX) {
        // If the packet is for the block after the next block (i.e. current
        // block + 2 blocks)
        if(pkt_mcnt_dist >= 2*N_TIME_PER_BLOCK*TIME_DEMUX) {
            // Mark the current block as filled
            netmcnt = set_block_filled(paper_input_databuf_p, &binfo);

            // Advance mcnt_start to next block
            cur_mcnt += N_TIME_PER_BLOCK*TIME_DEMUX;
            binfo.mcnt_start += N_TIME_PER_BLOCK*TIME_DEMUX;
            binfo.block_i = (binfo.block_i + 1) % N_INPUT_BLOCKS;

            // Wait (hopefully not long!) to acquire the block after next (i.e.
            // the block that gets the current packet).
            if(paper_input_databuf_busywait_free(paper_input_databuf_p, pkt_block_i) != HASHPIPE_OK) {
                if (errno == EINTR) {
                    // Interrupted by signal, return -1
                    hashpipe_error(__FUNCTION__, "interrupted by signal waiting for free databuf");
                    pthread_exit(NULL);
                    return -1; // We're exiting so return value is kind of moot
                } else {
                    hashpipe_error(__FUNCTION__, "error waiting for free databuf");
                    pthread_exit(NULL);
                    return -1; // We're exiting so return value is kind of moot
                }
            }

            // Initialize the newly acquired block
            initialize_block(paper_input_databuf_p, pkt_mcnt);
            // Reset binfo's packet counter for this packet's block
            binfo.block_packet_counter[pkt_block_i] = 0;
        }

        // Reset out-of-seq counter
        binfo.out_of_seq_cnt = 0;

        // Increment packet count for block
        binfo.block_packet_counter[pkt_block_i]++;
#ifdef LOG_MCNTS
        expected_packets_counted++;
#endif

        // Validate header FID and XID and calculate "m" and "f" indexes into
        // block (stored in binfo).
        if(calc_block_indexes(&binfo, &pkt_header)) {
            // Bad packet, error already reported
            return -1;
        }


        // Copy data into buffer
        payload_p = ((struct hera_ibv_feng_pkt *)p_frame)->payload;
        for(i=0; i<N_INPUTS_PER_PACKET/2; i++) {
            // Calculate starting points for unpacking this packet into block's data buffer.
            dest_p = (uint64_t *)(paper_input_databuf_p->block[pkt_block_i].data)
                + paper_input_databuf_data_idx(binfo.m, binfo.a + i, binfo.c, 0); //time index is always zero
            //fprintf(stdout, "m:%d, a:%d, c:%d, %lu\n", binfo.m, binfo.a, binfo.c, paper_input_databuf_data_idx(binfo.m, binfo.a, binfo.c, 0));
            memcpy_nt(dest_p, payload_p, 2*N_CHAN_PER_PACKET*N_TIME_PER_PACKET);
            payload_p += (2 * N_CHAN_PER_PACKET * N_TIME_PER_PACKET) / sizeof(uint64_t);
        }

        return netmcnt;
    }
    // Else, if packet is late, but not too late (so we can handle F engine
    // restarts and MCNT rollover), then ignore it
    else if(pkt_mcnt_dist < 0 && pkt_mcnt_dist > -LATE_PKT_MCNT_THRESHOLD) {
        // If not just after an mcnt reset, issue warning.
        if(cur_mcnt >= binfo.mcnt_log_late) {
            // We throttle these potentially very bursty messages, which
            // involves two steps:
            //
            // Start a new burst if the previous one has ended
            if(time(NULL) - burst_start > burst_max_duration_secs) {
                // Start a new burst
                burst_start = time(NULL);
                burst_message_counter = 0;
            }
            // If we have not yet logged the max number of messages for the
            // current burst, log this one and increment counter (and print
            // notification if threshold was reached).
            if(burst_message_counter < burst_message_threshold ) {
                hashpipe_warn("hera_ibvpkt_thread",
                        "Ignoring late packet (%d mcnts late, %d ant)",
                        cur_mcnt - pkt_mcnt, pkt_header.ant);
                burst_message_counter++;
                if(burst_message_counter == burst_message_threshold) {
                    hashpipe_warn("hera_ibvpkt_thread",
                            "suppressing further occurrences for %d seconds",
                            burst_start+burst_max_duration_secs-time(NULL));
                }
            }
        }
#ifdef LOG_MCNTS
        late_packets_counted++;
#endif
        return -1;
    }
    // Else, it is an "out-of-order" packet.
    else {
        // If not at start-up and this is the first out of order packet,
        // issue warning.
        if(cur_mcnt != 0 && binfo.out_of_seq_cnt == 0) {
            // We throttle these potentially very bursty messages, which
            // involves two steps:
            //
            // Start a new burst if the previous one has ended
            if(time(NULL) - burst_start > burst_max_duration_secs) {
                // Start a new burst
                burst_start = time(NULL);
                burst_message_counter = 0;
            }
            // If we have not yet logged the max number of messages for the
            // current burst, log this one and increment counter (and print
            // notification if threshold was reached).
            if(burst_message_counter < burst_message_threshold ) {
                hashpipe_warn("hera_ibvpkt_thread",
                        "out of seq mcnt %012lx from ant %d (expected mcnt: %012lx <= mcnt < %012x)",
                        pkt_mcnt, pkt_header.ant, cur_mcnt, cur_mcnt+3*N_TIME_PER_BLOCK*TIME_DEMUX);
                burst_message_counter++;
                if(burst_message_counter == burst_message_threshold) {
                    hashpipe_warn("hera_ibvpkt_thread",
                            "suppressing further occurrences for %d seconds",
                            burst_start+burst_max_duration_secs-time(NULL));
                }
            }
        }

        // Increment out-of-seq packet counter
        binfo.out_of_seq_cnt++;
#ifdef LOG_MCNTS
        outofseq_packets_counted++;
#endif

        // If too may out-of-seq packets
        if(binfo.out_of_seq_cnt > MAX_OUT_OF_SEQ) {
            // Reset current mcnt.  The value to reset to must be the first
            // value greater than or equal to pkt_mcnt that corresponds to the
            // same databuf block as the old current mcnt.
            if(binfo.block_i > pkt_block_i) {
                // Advance pkt_mcnt to correspond to binfo.block_i
                pkt_mcnt += TIME_DEMUX*N_TIME_PER_BLOCK*(binfo.block_i - pkt_block_i);
            } else if(binfo.block_i < pkt_block_i) {
                // Advance pkt_mcnt to binfo.block_i + N_INPUT_BLOCKS blocks
                pkt_mcnt += TIME_DEMUX*N_TIME_PER_BLOCK*(binfo.block_i + N_INPUT_BLOCKS - pkt_block_i);
            }
            // Round pkt_mcnt down to nearest multiple of Nm
            binfo.mcnt_start = start_for_mcnt(pkt_mcnt);
            binfo.mcnt_log_late = binfo.mcnt_start + N_TIME_PER_BLOCK*TIME_DEMUX;
            binfo.block_i = block_for_mcnt(binfo.mcnt_start);
            hashpipe_warn("hera_ibvpkt_thread",
                    "resetting to mcnt %012lx block %d based on packet mcnt %012lx from ant %d",
                    binfo.mcnt_start, block_for_mcnt(binfo.mcnt_start), pkt_mcnt, pkt_header.ant);
            // Reinitialize/recycle our two already acquired blocks with new
            // mcnt values.
            initialize_block(paper_input_databuf_p, binfo.mcnt_start);
            initialize_block(paper_input_databuf_p, binfo.mcnt_start+TIME_DEMUX*N_TIME_PER_BLOCK);
            // Reset binfo's packet counters for these blocks.
            binfo.block_packet_counter[binfo.block_i] = 0;
            binfo.block_packet_counter[(binfo.block_i+1)%N_INPUT_BLOCKS] = 0;
        }
        return -1;
    }

    return netmcnt;
}

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

static int init(hashpipe_thread_args_t *args)
{
#if 0
    /* Read network params */
    char bindhost[80];
    int bindport = 8511;

    strcpy(bindhost, "0.0.0.0");
#endif

    hashpipe_ibvpkt_databuf_t *dbin = (hashpipe_ibvpkt_databuf_t *)args->ibuf;
    const char * thread_name = args->thread_desc->name;
    hashpipe_status_t st = args->st;

    // Verify that the IBVPKTSZ was specified as expected/requried
    if(hashpipe_ibvpkt_databuf_slot_offset(dbin, PKT_OFFSET_HERA_FENG_HEADER) %
            HASHPIPE_IBVPKT_PKT_CHUNK_ALIGNMENT_SIZE != 0
    || hashpipe_ibvpkt_databuf_slot_offset(dbin, PKT_OFFSET_HERA_FENG_PAYLOAD) %
            HASHPIPE_IBVPKT_PKT_CHUNK_ALIGNMENT_SIZE != 0) {
        errno = EINVAL;
        hashpipe_error(thread_name, "IBVPKTSZ!=%d,%d,[...]",
            PKT_OFFSET_HERA_FENG_HEADER, PKT_OFFSET_HERA_FENG_PAYLOAD -
            PKT_OFFSET_HERA_FENG_HEADER);
        return HASHPIPE_ERR_PARAM;
    }

    hashpipe_status_lock_safe(&st);
    // Record version
    hputs(st.buf, "GIT_VER", GIT_VERSION);
#if 0
    // Get info from status buffer if present (no change if not present)
    hgets(st.buf, "BINDHOST", 80, bindhost);
    hgeti4(st.buf, "BINDPORT", &bindport);
    // Store bind host/port info etc in status buffer
    hputs(st.buf, "BINDHOST", bindhost);
    hputi4(st.buf, "BINDPORT", bindport);
#endif
    hputu4(st.buf, "MISSEDFE", 0);
    hputu4(st.buf, "MISSEDPK", 0);
    hashpipe_status_unlock_safe(&st);

#ifndef TIMING_TEST
#if 0
    /* Set up pktsock */
    struct hashpipe_pktsock *p_ps = (struct hashpipe_pktsock *)
        malloc(sizeof(struct hashpipe_pktsock));

    if(!p_ps) {
        perror(__FUNCTION__);
        return -1;
    }

    // Make frame_size be a divisor of block size so that frames will be
    // contiguous in mapped mempory.  block_size must also be a multiple of
    // page_size.  Easiest way is to oversize the frames to be 16384 bytes, which
    // is bigger than we need, but keeps things easy.
    p_ps->frame_size = PKTSOCK_BYTES_PER_FRAME;
    // total number of frames
    p_ps->nframes = PKTSOCK_NFRAMES;
    // number of blocks
    p_ps->nblocks = PKTSOCK_NBLOCKS;

    int rv = hashpipe_pktsock_open(p_ps, bindhost, PACKET_RX_RING);
    if (rv!=HASHPIPE_OK) {
        hashpipe_error("hera_pktsock_thread", "Error opening pktsock.");
        pthread_exit(NULL);
    }

    // Store packet socket pointer in args
    args->user_data = p_ps;
#endif
#endif

    // Success!
    return 0;
}

static void *run(hashpipe_thread_args_t * args)
{
    // Local aliases to shorten access to args fields
    // Our input buffer is a hashpipe_ibvpkt_databuf_t
    hashpipe_ibvpkt_databuf_t *dbin = (hashpipe_ibvpkt_databuf_t *)args->ibuf;
    // Our output buffer happens to be a paper_input_databuf
    paper_input_databuf_t *db = (paper_input_databuf_t *)args->obuf;
    hashpipe_status_t st = args->st;
    const char * thread_name = args->thread_desc->name;
    const char * status_key = args->thread_desc->skey;

    st_p = &st; // allow global (this source file) access to the status buffer

    // Flag that holds off the net thread
    int holdoff = 1;

    // Variables for working with the input databuf
    struct hashpipe_pktbuf_info * pktbuf_info = hashpipe_ibvpkt_databuf_pktbuf_info_ptr(dbin);
    struct hashpipe_ibv_context * hibv_ctx = hashpipe_ibvpkt_databuf_hibv_ctx_ptr(dbin);
    int block_idx_in = 0;
    const int npkts_per_block_in = pktbuf_info->slots_per_block;
    const int slot_size = pktbuf_info->slot_size;
    // Force ourself into the hold off state and get network params
    fprintf(stdout, "Setting NETHOLD state to 1. Waiting for someone to set it to 0\n");
    hashpipe_status_lock_safe(&st);
    {
        hputi4(st.buf, "NETHOLD", 1);
        hputs(st.buf, status_key, "holding");
    }
    hashpipe_status_unlock_safe(&st);

    while(holdoff) {
        // We're not in any hurry to startup
        sleep(1);
        hashpipe_status_lock_safe(&st);
        // Look for NETHOLD value
        hgeti4(st.buf, "NETHOLD", &holdoff);
        // Get the time index of this correlator. I.e. is it correlating
        // even or odd blocks of samples.
        hgeti4(st.buf, "TIMEIDX", &time_index);
        if(!holdoff) {
            // Done holding, so delete the key
            hdel(st.buf, "NETHOLD");
            hputs(st.buf, status_key, "starting");
        }
        hashpipe_status_unlock_safe(&st);
    }

#ifdef DEBUG_SEMS
    fprintf(stderr, "s/tid %lu/NET/' <<.\n", pthread_self());
#endif

#if 0
    /* Copy status buffer */
    char status_buf[HASHPIPE_STATUS_SIZE];
    hashpipe_status_lock_busywait_safe(st_p);
    memcpy(status_buf, st_p->buf, HASHPIPE_STATUS_SIZE);
    hashpipe_status_unlock_safe(st_p);
#endif

    // Acquire first two blocks to start
    if(paper_input_databuf_busywait_free(db, 0) != HASHPIPE_OK) {
        if (errno == EINTR) {
            // Interrupted by signal, return -1
            hashpipe_error(__FUNCTION__, "interrupted by signal waiting for free databuf");
            pthread_exit(NULL);
        } else {
            hashpipe_error(__FUNCTION__, "error waiting for free databuf");
            pthread_exit(NULL);
        }
    }
    if(paper_input_databuf_busywait_free(db, 1) != HASHPIPE_OK) {
        if (errno == EINTR) {
            // Interrupted by signal, return -1
            hashpipe_error(__FUNCTION__, "interrupted by signal waiting for free databuf");
            pthread_exit(NULL);
        } else {
            hashpipe_error(__FUNCTION__, "error waiting for free databuf");
            pthread_exit(NULL);
        }
    }

    // Initialize the newly acquired block
    initialize_block(db, time_index);
    initialize_block(db, N_TIME_PER_BLOCK*TIME_DEMUX + time_index);

    /* Read network params */
    int bindport = 8511;
    // (N_BYTES_PER_PACKET excludes header, so +8 for the header)
    size_t expected_packet_size = N_BYTES_PER_PACKET + 8;

#ifndef TIMING_TEST
    hashpipe_status_lock_safe(&st);
    {
        // Get info from status buffer
        hgeti4(st.buf, "BINDPORT", &bindport);
        hputi4(st.buf, "BINDPORT", bindport);
        hputu4(st.buf, "MISSEDFE", 0);
        hputu4(st.buf, "MISSEDPK", 0);
        hputs(st.buf, status_key, "running");
    }
    hashpipe_status_unlock_safe(&st);
#endif

    /* Main loop */
    int i;
    uint64_t packet_count = 0;
    uint64_t wait_ns = 0; // ns for most recent wait
    uint64_t recv_ns = 0; // ns for most recent recv
    uint64_t proc_ns = 0; // ns for most recent proc
    uint64_t min_wait_ns = 99999; // min ns per single wait
    uint64_t min_recv_ns = 99999; // min ns per single recv
    uint64_t min_proc_ns = 99999; // min ns per single proc
    uint64_t max_wait_ns = 0;     // max ns per single wait
    uint64_t max_recv_ns = 0;     // max ns per single recv
    uint64_t max_proc_ns = 0;     // max ns per single proc
    uint64_t elapsed_wait_ns = 0; // cumulative wait time per block
    uint64_t elapsed_recv_ns = 0; // cumulative recv time per block
    uint64_t elapsed_proc_ns = 0; // cumulative proc time per block
    uint64_t status_ns = 0; // User to fetch ns values from status buffer
    float ns_per_wait = 0.0; // Average ns per wait over 1 block
    float ns_per_recv = 0.0; // Average ns per recv over 1 block
    float ns_per_proc = 0.0; // Average ns per proc over 1 block
    unsigned int pktsock_pkts = 0;  // Stats counter from socket packet
    unsigned int pktsock_drops = 0; // Stats counter from socket packet
    uint64_t pktsock_pkts_total = 0;  // Stats total for socket packet
    uint64_t pktsock_drops_total = 0; // Stats total for socket packet
    struct timespec start={0}, stop={0};
    struct timespec recv_start={0}, recv_stop={0};

    // Wait for ibvpkt thread to be running, then it's OK to add/remove flows.
    hashpipe_ibvpkt_databuf_wait_running(&st);

    // Add flow to grab all packets sent to my MAC address with UDP port
    // `bindport` (e.g. 8511)
    if(hashpipe_ibv_flow(hibv_ctx, 0, IBV_FLOW_SPEC_UDP,
                hibv_ctx->mac, NULL, 0, 0,
                0, 0, 0, bindport))
    {
        hashpipe_error(thread_name, "hashpipe_ibv_flow error");
        return NULL;
    }

    while (run_threads()) {

#ifndef TIMING_TEST
        // Wait for input block to be filled

        if(hashpipe_ibvpkt_databuf_wait_filled(dbin, block_idx_in)) {
            hashpipe_error(thread_name, "error waiting for input block %d", block_idx_in);
            break;
        }

        if(!run_threads()) {
            // We're outta here!
            // (but first mark the block free)
            hashpipe_ibvpkt_databuf_set_free(dbin, block_idx_in);
            break;
        }

        // Process packets from block
        unsigned char * p_frame = (unsigned char *)hashpipe_ibvpkt_databuf_data(dbin, block_idx_in);
        for(i=0; i < npkts_per_block_in; i++, p_frame += slot_size) {
            // TODO Check packet size (needs IBVPKT_UDP_SIZE)
#if 0
            // Make sure received packet size matches expected packet size.  Allow
            // for optional 8 byte CRC in received packet.  Zlib's crc32 function
            // is too slow to use in realtime, so CRCs cannot be checked on the
            // fly.  If data errors are suspected, a separate CRC checking utility
            // should be used to read the packets from the network and verify CRCs.
            int packet_size = IBVPKT_UDP_SIZE(p_frame) - 8; // -8 for the UDP header
            if (expected_packet_size != packet_size-8 && expected_packet_size != packet_size) {
                // Log warning and ignore wrongly sized packet
                #ifdef DEBUG_NET
                hashpipe_warn("hera_ibvpkt_thread", "Invalid pkt size (%d)", packet_size);
                #endif
                continue;
            }
#endif // 0
#endif
            packet_count++;

            // Copy packet into any blocks where it belongs.
            const uint64_t mcnt = process_packet((paper_input_databuf_t *)db, p_frame);

            clock_gettime(CLOCK_MONOTONIC, &stop);
            wait_ns = ELAPSED_NS(recv_start, start);
            recv_ns = ELAPSED_NS(start, recv_stop);
            proc_ns = ELAPSED_NS(recv_stop, stop);
            elapsed_wait_ns += wait_ns;
            elapsed_recv_ns += recv_ns;
            elapsed_proc_ns += proc_ns;
            // Update min max values
            min_wait_ns = MIN(wait_ns, min_wait_ns);
            min_recv_ns = MIN(recv_ns, min_recv_ns);
            min_proc_ns = MIN(proc_ns, min_proc_ns);
            max_wait_ns = MAX(wait_ns, max_wait_ns);
            max_recv_ns = MAX(recv_ns, max_recv_ns);
            max_proc_ns = MAX(proc_ns, max_proc_ns);

            if(mcnt != -1) {
                // Update status
                ns_per_wait = (float)elapsed_wait_ns / packet_count;
                ns_per_recv = (float)elapsed_recv_ns / packet_count;
                ns_per_proc = (float)elapsed_proc_ns / packet_count;

                hashpipe_status_lock_busywait_safe(&st);

                hputu8(st.buf, "NETMCNT", mcnt);
#if 0
                // Gbps = bits_per_packet / ns_per_packet
                // (N_BYTES_PER_PACKET excludes header, so +8 for the header)
                hputr4(st.buf, "NETGBPS", 8*(N_BYTES_PER_PACKET+8)/(ns_per_recv+ns_per_proc));
                hputr4(st.buf, "NETWATNS", ns_per_wait);
                hputr4(st.buf, "NETRECNS", ns_per_recv);
                hputr4(st.buf, "NETPRCNS", ns_per_proc);

                // Get and put min and max values.  The "get-then-put" allows the
                // user to reset the min max values in the status buffer.
                hgeti8(st.buf, "NETWATMN", (long int *)&status_ns);
                status_ns = MIN(min_wait_ns, status_ns);
                hputi8(st.buf, "NETWATMN", status_ns);

                hgeti8(st.buf, "NETRECMN", (long int *)&status_ns);
                status_ns = MIN(min_recv_ns, status_ns);
                hputi8(st.buf, "NETRECMN", status_ns);

                hgeti8(st.buf, "NETPRCMN", (long int *)&status_ns);
                status_ns = MIN(min_proc_ns, status_ns);
                hputi8(st.buf, "NETPRCMN", status_ns);

                hgeti8(st.buf, "NETWATMX", (long int *)&status_ns);
                status_ns = MAX(max_wait_ns, status_ns);
                hputi8(st.buf, "NETWATMX", status_ns);

                hgeti8(st.buf, "NETRECMX", (long int *)&status_ns);
                status_ns = MAX(max_recv_ns, status_ns);
                hputi8(st.buf, "NETRECMX", status_ns);

                hgeti8(st.buf, "NETPRCMX", (long int *)&status_ns);
                status_ns = MAX(max_proc_ns, status_ns);
                hputi8(st.buf, "NETPRCMX", status_ns);

                hputu8(st.buf, "NETPKTS",  pktsock_pkts);
                hputu8(st.buf, "NETDROPS", pktsock_drops);

                hgetu8(st.buf, "NETPKTTL", (long unsigned int*)&pktsock_pkts_total);
                hgetu8(st.buf, "NETDRPTL", (long unsigned int*)&pktsock_drops_total);
                hputu8(st.buf, "NETPKTTL", pktsock_pkts_total + pktsock_pkts);
                hputu8(st.buf, "NETDRPTL", pktsock_drops_total + pktsock_drops);
#endif

                hashpipe_status_unlock_safe(&st);

                // Start new average
                elapsed_wait_ns = 0;
                elapsed_recv_ns = 0;
                elapsed_proc_ns = 0;
                packet_count = 0;
            }
        }

        // Mark input block as free
        hashpipe_ibvpkt_databuf_set_free(dbin, block_idx_in);

        // Advance to next input block
        block_idx_in = (block_idx_in + 1) % dbin->header.n_block;

        /* Will exit if thread has been cancelled */
        pthread_testcancel();
    }

    return NULL;
}

static hashpipe_thread_desc_t ibvpkt_thread = {
    name: "hera_ibvpkt_thread",
    skey: "NETSTAT",
    init: init,
    run:  run,
    ibuf_desc: {hashpipe_ibvpkt_databuf_create},
    obuf_desc: {paper_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&ibvpkt_thread);
}

// vi: set ts=8 sw=4 noet :
