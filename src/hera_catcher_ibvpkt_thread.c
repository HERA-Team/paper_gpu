/* hera_hera_catcher_ibvpkt_thread.c
 *
 * Routine to process catcher packets from data buffer populated by
 * hashpipe_ibvpkt_thread.
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
#include "hashpipe_packet.h"

// The HERA X-Engine packets arrive over the wire with this layout (first 64
// bytes shown):
//
//     0x0000:  0202 0a50 28c8 0033 eba3 1701 0800 4500
//     0x0010:  1224 0000 4000 ff11 040c 0a50 2855 0a50
//     0x0020:  28c8 2142 213f 1210 0000 0226 a140 8f00
//     0x0030:  0057 7d7e 7171 3131 d9d9 e4d4 2222 3737
//     0x0040:  2717 9191 c4c5 2f2f 0f1f 6969 7979 a191
//
// This consists of a 42 byte network (eth/ip/udp) header, an 8 byte payload
// header, and 4096(?) bytes of payload data (treated as an array of uint32_t).
//
// By using `IBVPKTSZ=42,24,4096`, each of these regions is padded to the next
// multiple of 64, so this packet gets stored in memory like this:
//
//     0x0000:  0202 0a50 28c8 0033 eba3 1701 0800 4500
//     0x0010:  1224 0000 4000 ff11 040c 0a50 2855 0a50
//     0x0020:  28c8 2142 213f 1210 0000 ---- ---- ----
//     0x0030:  ---- ---- ---- ---- ---- ---- ---- ----
//     0x0040:  MMMM MMMM MMMM MMMM BBBB BBBB OOOO OOOO
//     0x0050:  A0A0 A1A1 XXXX LLLL ---- ---- ---- ----
//     0x0060:  ---- ---- ---- ---- ---- ---- ---- ----
//     0x0070:  ---- ---- ---- ---- ---- ---- ---- ----
//     0x0080:  7d7e 7171 3131 d9d9 e4d4 2222 3737 2717
//     0x0090:  9191 c4c5 2f2f 0f1f 6969 7979 a191 ....

typedef struct {
    uint64_t mcnt;        // timestamp of the packet
    uint32_t bcnt;        // unique id based on order of baselines sent to catcher
    uint32_t offset;      // channel within one x-eng
    uint16_t ant0;
    uint16_t ant1;
    uint16_t xeng_id;	  // For time demux and starting channel
    uint16_t payload_len; // should be 4096
} packet_header_t;

// HERA X-Engine packet with link layer header and internal padding to optimize
// alignment.  The alignment is acheived through judicious use of IB Verbs
// scatter/gather capabilities (specifically the scatter part).
struct __attribute__ ((__packed__)) hera_ibv_xeng_pkt {
  struct ethhdr ethhdr;
  struct iphdr iphdr;
  struct udphdr udphdr;
  uint8_t pad0[22];
  packet_header_t hdr;
  uint8_t pad1[40];
  uint32_t payload[]; // TODO Provide payload length? (OUTPUT_BYTES_PER_PACKET/sizeof(uint32_t))?
};

// HERA X-Engine header byte offset within (unpadded) packet
#define PKT_OFFSET_HERA_XENG_HEADER \
  (sizeof(struct ethhdr) + \
   sizeof(struct iphdr ) + \
   sizeof(struct udphdr))

// HERA X-Engine payload byte offset within (unpadded) packet
#define PKT_OFFSET_HERA_XENG_PAYLOAD \
  (PKT_OFFSET_HERA_XENG_HEADER + 3*sizeof(uint64_t))

#define MAX_OUT_OF_SEQ_PKTS           (4096)

// This allows packets to be two full databufs late without being considered
// out of sequence.
// ARP: dont think this is a tight enough threshold
#define LATE_PKT_BCNT_THRESHOLD (2*BASELINES_PER_BLOCK*CATCHER_N_BLOCKS)

#define DEBUG_NET

#ifndef MIN
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

#ifndef MAX
#define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif

#if 0
//#define PKTSOCK_BYTES_PER_FRAME (16384)
//#define PKTSOCK_FRAMES_PER_BLOCK (8)
//#define PKTSOCK_NBLOCKS (800)
//#define PKTSOCK_NFRAMES (PKTSOCK_FRAMES_PER_BLOCK * PKTSOCK_NBLOCKS)
#define PKTSOCK_BYTES_PER_FRAME (4864)
#define PKTSOCK_FRAMES_PER_BLOCK (128)
#define PKTSOCK_NBLOCKS (5000)
#define PKTSOCK_NFRAMES (PKTSOCK_FRAMES_PER_BLOCK * PKTSOCK_NBLOCKS)
#endif // 0

// The fields of a block_info_t structure hold meta-data about the contents of
// all the blocks in the output databuf.  As you can see by the
// `CATCHER_N_BLOCKS` in the dimesions of some of the array fields, a single
// instance of this block_info_t structure handles all the blocks of the output
// databuf.  In fact, this thread actually has only a single block_info_t
// structure as a static local variable in `process_packet()`.
typedef struct {
    int initialized;
    uint32_t bcnt_start;
    int block_i; // ranges from 0 to CATCHER_N_BLOCKS-1
    uint64_t bcnt_log_late;
    long out_of_seq_cnt;
    long block_packet_counter[CATCHER_N_BLOCKS];
    long xeng_pkt_counter[CATCHER_N_BLOCKS][N_XENGINES];
    char flags[CATCHER_N_BLOCKS][PACKETS_PER_BLOCK];
    char baselines[CATCHER_N_BLOCKS][BASELINES_PER_BLOCK];
} block_info_t;

// There are many fields and variables related to `bcnt` (Baseline CouNT).
// Each integration dumped by an X engine has `Nbls = Nants_data *
// (Nants_data+1) / 2` baselines.  These baselines have a `dump_count`
// (does that have a real name?) that ranges from 0 through Nbls-1 within a
// given dump.  `bcnt` is a monotonically increasing counter that doesn't reset
// between dumps, so the dumped baselines have an absolute (global) `bcnt` of
// `dump_count * Nbls + dump_bcnt`, where dump_count counts dumps from 0.
// `bcnt` is always(?) represented as a `uint32_t`.
//
// Every output databuf block has a header that contains a `bcnt` field that is
// an array of length `BASELINES_PER_BLOCK` with each entry containing the
// `bcnt` of the corresponding "slice" of the data portion of the databuf block.
//
// hera_catcher_net_thread and hera_catcher_ibvpkt_thread are blissfully
// unaware of `Nbls`. Instead the arrange the data from incoming packets into
// sequence of spectra for consecutive bcnts, partitioning them into
// `BASELINES_PER_BLOCK` `bcnt`s per block.
//
// The various `bcnt` related fields and variables are elucidated here:
//
// - binfo.bcnt_start (static local in `process_packet()`)
//   The first `bcnt` of the next output databuf block to be marked filled.
//   * `binfo` is the singleton instance of `block_info_t` that the declared as a
//     static local variable in `process_packet().
//   * Initialized to the very first processed packet's `bcnt` rounded down to
//     the previous multiple of `BASELINES_PER_BLOCK`.
//   * Incremented by `BASELINES_PER_BLOCK` every time an output block is
//     marked filled.
//   * Re-initialized to the processed packet's `bcnt` rounded down to
//     the previous multiple of `BASELINES_PER_BLOCK` when resetting due to too
//     many out of sequence packets.
//
// - binfo.bcnt_log_late (static local in `process_packet()`)
//   A `bcnt` threshold used to squelch late packet warnings at startup/reset.
//   * Initialized to `BASELINES_PER_BLOCK`.
//   * Re-initialized to `binfo.bcnt_start + 2*BASELINES_PER_BLOCK` when
//     resetting due to too many out of sequence packets.
//
// - first_bcnt (file static, aka file global)
//   The first `bcnt` value of output databuf block 0.  Always a multiple of
//   `BASELINES_PER_BLOCK`.
//   * (Re-)initialized to 0 on the very first processed packet.
//   * Re-initialized to the processed packets `bcnt` shifted down by (i.e.
//     minus) `BASELINES_PER_BLOCK * binfo.block_i` (i.e. `BASELINES_PER_BLOCK`
//     time the next data buffer output block number to be marked filled?) all
//     rounded down to the previous multiple of `BASELINES_PER_BLOCK`.
//   * Used in `block_for_bcnt()` to determine which output databuf block
//     corresponds to a given `bcnt`.
//
// - pkt_header.bcnt (process packet local)
//   Every packet that the catcher receives from the X engines has a `bcnt`
//   value in its header.
//
// - pkt_bcnt_dist (process_packet local)
//   The "distance" from `binfo.bcnt_start` to the processed packet's `bcnt`.
//   * Calculated as `pkt_header.bcnt - binfo.bcnt_start`.
//   * Packets with `pkt_bcnt_dist` greater than or equal to 0 and less that
//     `3*BASELINES_PER_BLOCK` are accepted.
//   * For accepted packets, currently, if `pkt_bcnt_dist` is greater than or
//     equal to `2*BASELINES_PER_BLOCK`, then:
//     1. The current block is marked as filled
//     2. `binfo` fields `bcnt_start` and `block_i` are advanced by one block
//     3. The "per block" `binfo` fields of the new `pkt_block_i` block are set
//        to zeros (`flags` set to ones), as is the `binfo.out_of_seq_cnt`
//        field.
//     4. A new block is acquired (wait free) and the block is "initialized"
//        via `initialize_block()` with `binfo.bcnt_start+BASELINES_PER_BLOCK`.
//   * For all accepted packets:
//     1. The payload is copied into the packet's slice in the packet's
//        `bcnt`'s block.
//     2. `binfo` block-baseline fields are updated if this is the first packet
//        for this block-baseline.
//     3. `binfo.flags` for this packet's block and offset are set to 0.
//     4. `binfo.block_packet_counter` for the packet's block in incremented.
//   * Unaccepted packets that are "late" (i.e. for a block already marked
//     filled) but not too late get ignored.  The late packet is logged if
//     binfo.bcnt_start has advanced beyond binfo.bcnt_log_late.
//   * Other unaccepted packets are either way too late or too far in the
//     future to be useful.  Increment `binfo.out_of_seq_cnt` and if it execeds
//     a threshold, reset:
//     1. Re-initialize `binfo.bcnt_start`, `first_bcnt`,
//        `binfo.bcnt_log_late`, and `binfo.out_of_seq_cnt`.
//     2. Re-initialize `binfo` counters and stats to zeros (flags to ones).
//     3. Re-initialize the two "working blocks" vis `initialize_block()`.
//
// - netbcnt (process_packet local)
//   `bcnt` value to be returned by `process_packet`.

static hashpipe_status_t *st_p;
static const char * status_key;
static uint32_t first_bcnt = 0;

// Get physical block number given the bcnt
static inline int block_for_bcnt(uint32_t bcnt){
    return ((bcnt-first_bcnt) / BASELINES_PER_BLOCK) % CATCHER_N_BLOCKS;
}

// Initialize a block by clearing its "good data" flag and saving the
// bcnt of the first baseline in this block. The bcnt should be a multiple
// of BASELINES_PER_BLOCK.
static inline void initialize_block(hera_catcher_bda_input_databuf_t * db, uint64_t bcnt){
  int block_i = block_for_bcnt(bcnt);
  db->block[block_i].header.bcnt[0]   = bcnt;
  db->block[block_i].header.good_data = 0;
}

/* Get packet header */
static inline void get_header(unsigned char *p_frame, packet_header_t *pkt_header){
   packet_header_t *packet_header_raw = &((struct hera_ibv_xeng_pkt *)p_frame)->hdr;
   pkt_header->mcnt        = be64toh(packet_header_raw->mcnt);
   pkt_header->bcnt        = be32toh(packet_header_raw->bcnt);
   pkt_header->offset      = be32toh(packet_header_raw->offset);
   pkt_header->ant0        = be16toh(packet_header_raw->ant0);
   pkt_header->ant1        = be16toh(packet_header_raw->ant1);
   pkt_header->xeng_id     = be16toh(packet_header_raw->xeng_id);
   pkt_header->payload_len = be16toh(packet_header_raw->payload_len);
}

/* Set hashpipe block to filled */
// This sets the "current" block to be marked as filled.
// Returns bcnt of the block being marked filled.
static uint32_t set_block_filled(hera_catcher_bda_input_databuf_t *db, block_info_t *binfo){
  static int last_filled = -1;

  uint64_t block_missed_pkt_cnt;
  uint64_t block_missed_xengs, block_missed_mod_cnt, missed_pkt_cnt=0;
  uint32_t block_i = block_for_bcnt(binfo->bcnt_start);
  //int i;

  // Validate that we're filling blocks in the proper sequence
  // ARP don't understand last_filled math here (isn't it -1?). Also, this should work so
  // it is impossible to screw up the order.
  // looks like it is implicitly tied to starting at block_i=0, and then advances
  // along with it, as long as set_block_filled is only called once per block
  // but if we skip a block, we never set filled, and the disk writer will lock
  last_filled = (last_filled+1) % CATCHER_N_BLOCKS;
  if(last_filled != block_i) {
    printf("block %d being marked filled, but expected block %d!\n", block_i, last_filled);
  }
  //printf("net_thread: marking block_i=%d filled\n", block_i);

  // Validate that block_i matches binfo->block_i
  // Should never mismatch since block_i comes from binfo->bcnt_start which is
  // always updated in tandem with binfo->block_i.
  if(block_i != binfo->block_i) {
    hashpipe_warn(__FUNCTION__,
        "block_i for binfo's bcnt (%d) != binfo's block_i (%d)",
        block_i, binfo->block_i);
  }

  // If all packets are accounted for, mark this block as good
  if(binfo->block_packet_counter[block_i] == PACKETS_PER_BLOCK){
    db->block[block_i].header.good_data = 1;
  }

  // Set the block as filled
  if(hera_catcher_bda_input_databuf_set_filled(db, block_i) != HASHPIPE_OK){
    hashpipe_error(__FUNCTION__, "error waiting for databuf filled call");
    pthread_exit(NULL);
  }

  // Calculate missing packets.
  block_missed_pkt_cnt = PACKETS_PER_BLOCK - binfo->block_packet_counter[block_i];

  block_missed_xengs = block_missed_pkt_cnt / PACKETS_PER_X ;
  block_missed_mod_cnt = block_missed_pkt_cnt % PACKETS_PER_X ;

  //fprintf(stdout,"Missed packets: %ld\tMissed Xengs:%ld\t", block_missed_pkt_cnt, block_missed_xengs);

  // Update status buffer
  hashpipe_status_lock_busywait_safe(st_p);
  hputu4(st_p->buf, "NETBKOUT", block_i);
  hputu4(st_p->buf, "MISSXENG", block_missed_xengs);
  if(block_missed_mod_cnt){
    // Increment MISSEDPK by number of missed packets for this block
    hgetu8(st_p->buf, "MISSEDPK", &missed_pkt_cnt);
    missed_pkt_cnt += block_missed_pkt_cnt;
    hputu8(st_p->buf, "MISSEDPK", missed_pkt_cnt);
  }
  hashpipe_status_unlock_safe(st_p);

  // Do block_missind_mod_cnt I/O outside of status buffer lock
  if(block_missed_mod_cnt){
    fprintf(stderr, "BCNT %d: Expected %lu packets, Got %lu\n",
        binfo->bcnt_start,
        PACKETS_PER_BLOCK,
        binfo->block_packet_counter[block_i]);
    // Print stats per-xeng
    //fprintf(stderr, "Fraction pkts received:\n");
    //for (i=0; i<N_XENGINES; i++){
    //  // ARP: this print is numerically incorrect. xeng_pkt_counter is never incremented
    //  fprintf(stderr, "XengID %2d: %.2f\n", i,
    //          (float)binfo->xeng_pkt_counter[block_i][i]/PACKETS_PER_X);
    //}
  }

  return db->block[block_i].header.bcnt[0];
}

/* Initialize block_info */
// This function must be called once and only once per block_info structure!
// This hashpipe thread uses only one block_info instance and calls this
// function upon reception of the very first packet.
// Subsequent calls are no-ops.
static inline void initialize_block_info(block_info_t * binfo, uint32_t bcnt){
    int i;

    // If this block_info structure has already been initialized
    if(binfo->initialized) {
        return;
    }

    binfo->initialized    = 1;
    binfo->bcnt_start     = bcnt - (bcnt % BASELINES_PER_BLOCK);
    binfo->block_i        = block_for_bcnt(bcnt); // Depends on first_bcnt!
    binfo->bcnt_log_late  = BASELINES_PER_BLOCK;
    binfo->out_of_seq_cnt = 0;

    for(i = 0; i < CATCHER_N_BLOCKS; i++) {
      binfo->block_packet_counter[i] = 0;
      memset(binfo->xeng_pkt_counter[i], 0, N_XENGINES*sizeof(long));
      memset(binfo->flags[i], 1, PACKETS_PER_BLOCK*sizeof(char));
      memset(binfo->baselines[i], 0, BASELINES_PER_BLOCK*sizeof(char));
    }
}

/* Process packet */
// This function returns -1 unless the given packet causes a block to be marked
// as filled in which case this function returns the marked block's first bcnt.
// Any return value other than -1 will be stored in the status memory as
// NETMCNT, so it is important that values other than -1 are returned rarely
// (i.e. when marking a block as filled)!!!

static inline uint32_t process_packet(
  hera_catcher_bda_input_databuf_t *db, unsigned char *p_frame){

  static block_info_t binfo = {0};
  packet_header_t pkt_header;
  int pkt_block_i=0;
  const uint32_t *payload_p;
  uint32_t *dest_p;
  int32_t pkt_bcnt_dist;
  uint64_t pkt_mcnt;
  uint32_t netbcnt = -1; // Value to return unless block is filled
  int b, x, t, o;
  int rv;
  uint32_t pkt_offset;
  int time_demux_block;

  // Parse packet header
  get_header(p_frame, &pkt_header);

  // Lazy init binfo
  if(!binfo.initialized){
    // This is the very first packet received
    fprintf(stdout,"Initializing binfo..\n");

    // Set first_bcnt to "pkt_header.bcnt rounded down to closest multiple of
    // BASELINES_PER_BLOCK".  first_bcnt must be set (and valid) before calling
    // initialize_block_info() because that function ends up using first_bcnt.
    first_bcnt = pkt_header.bcnt - (pkt_header.bcnt % BASELINES_PER_BLOCK);

    // Now we can initialize block info based on pkt_header.bcnt
    initialize_block_info(&binfo, pkt_header.bcnt);

    // Initialize the newly acquired blocks
    // Note that these blocks aren't really so "newly" acquired since they were
    // acquired before entering the main "for each packet" loop and these
    // initialize_block calls only happen upon reception of the very first
    // packet.
    fprintf(stdout,"Initializing the first blocks..\n");
    initialize_block(db, pkt_header.bcnt);
    initialize_block(db, pkt_header.bcnt+BASELINES_PER_BLOCK);
  } // end of "first packet ever" block

  // This MUST come after the "first packet ever" block because
  // block_for_bcnt() uses first_bcnt which is initialized in the "first packet
  // ever" block.
  pkt_block_i = block_for_bcnt(pkt_header.bcnt);

  //fprintf(stdout, "curr:%d\tnext:%d\t",binfo.block_curr, binfo.block_next);
  //fprintf(stdout, "bcnt:%d\tblock_id:%d\n",pkt_header.bcnt, pkt_block_i);
  //fprintf(stdout, "xeng:%d\n",pkt_header.xeng_id);

  // Packet bcnt distance (how far away is this packet's bcnt from the
  // current bcnt).  Positive distance for packet bcnt > current mcnt.
  // ARP: prefer comparing pkt_block_i to binfo.block_i to trigger advance
  pkt_bcnt_dist = pkt_header.bcnt - binfo.bcnt_start;

  // We expect packets for the current block (0) and the next block (1). If a packet
  // belonging to the block after (2) arrives, the current block is marked full and
  // counters advance (1,2,3).
  // ARP: currently tuned to transmissions don't overlap at all, so could reduce this to 1
  if (0 <= pkt_bcnt_dist && pkt_bcnt_dist < 3*BASELINES_PER_BLOCK){
    // If the packet is for the block after the next block (i.e. current block
    // + 2 blocks), mark the current block as filled.
    if (pkt_bcnt_dist >= 2*BASELINES_PER_BLOCK){

       // Set current block filled
       netbcnt = set_block_filled(db, &binfo);
       //fprintf(stdout,"Filled Block: %d from bcnt: %d to bcnt: %d\n", binfo.block_i, db->block[binfo.block_i].header.bcnt[0],
       //                                                               db->block[binfo.block_i].header.bcnt[BASELINES_PER_BLOCK-1]);

       // Update binfo's "current block" fields
       //printf("net_thread: block_i=%d -> %d\n", binfo.block_i, (binfo.block_i + 1) % CATCHER_N_BLOCKS);
       binfo.bcnt_start += BASELINES_PER_BLOCK;
       binfo.block_i = (binfo.block_i+1) % CATCHER_N_BLOCKS;
       // TODO binfo.block_log_late = ???;
       binfo.out_of_seq_cnt = 0;

       // At this point, pkt_block_i should be the block after binfo.block_i
       if(pkt_block_i != (binfo.block_i + 1) % CATCHER_N_BLOCKS) {
         hashpipe_warn(__FUNCTION__, "expected next block to be %d, but got %d",
             (binfo.block_i + 1) % CATCHER_N_BLOCKS, pkt_block_i);
       }

       // Reset counters for pkt_block_i ("new" block)
       binfo.block_packet_counter[pkt_block_i] = 0;
       memset(binfo.xeng_pkt_counter[pkt_block_i], 0, N_XENGINES*sizeof(long));
       memset(binfo.flags[pkt_block_i],            1, PACKETS_PER_BLOCK*sizeof(char));
       memset(binfo.baselines[pkt_block_i],        0, BASELINES_PER_BLOCK*sizeof(char));

       // Wait (hopefully not long!) to acquire the block after next.
       while((rv=hera_catcher_bda_input_databuf_busywait_free(db, pkt_block_i)) != HASHPIPE_OK) {
         if (rv == HASHPIPE_TIMEOUT){
             pthread_exit(NULL);
             return -1;
         }
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
       initialize_block(db, binfo.bcnt_start+BASELINES_PER_BLOCK);

       hashpipe_status_lock_safe(st_p);
       hputs(st_p->buf, status_key, "running");
       hashpipe_status_unlock_safe(st_p);
    }

    // Evaluate the location in the buffer to which to copy the packet data
    b = (pkt_header.bcnt - binfo.bcnt_start) % BASELINES_PER_BLOCK;
    x = pkt_header.xeng_id % N_XENGINES_PER_TIME;
    t = (pkt_header.mcnt/Nt) % TIME_DEMUX;  //Nt = 2
    o = pkt_header.offset;
    pkt_offset = hera_catcher_bda_input_databuf_pkt_offset(b, t, x, o);
    //fprintf(stdout, "bcnt-loc:%d\txeng:%d\ttime:%d\tpktoffset:%d\n",b,x,t,o);
    //fprintf(stdout, "offset: %d\n", pkt_offset);

    // Check for duplicate packets (i.e. has flag already been cleared to 0?)
    if(!binfo.flags[pkt_block_i][pkt_offset]){
      // This slot is already filled
      fprintf(stderr, "Packet repeated!!\n");
      binfo.out_of_seq_cnt++;
      return -1;
    }

    // Copy data into buffer
    payload_p = ((struct hera_ibv_xeng_pkt *)p_frame)->payload;
    dest_p    = (uint32_t *)(db->block[pkt_block_i].data + (pkt_header.payload_len * pkt_offset/sizeof(uint32_t)));
    memcpy(dest_p, payload_p, pkt_header.payload_len);

    //fprintf(stdout,"bcnt:%d\t block_id:%d\t Pkt cntr: %lu\n",b, pkt_block_i, binfo.block_packet_counter[pkt_block_i]);

    // If this is the first packet of this baseline, update header
    if(!binfo.baselines[pkt_block_i][b]){
      // Split the mcnt into a "pkt_mcnt" which is the same for all even/odd samples,
      // and "time_demux_block", which indicates which even/odd block this packet came from
      time_demux_block = (pkt_header.mcnt / Nt) % TIME_DEMUX;
      pkt_mcnt = pkt_header.mcnt - (Nt*time_demux_block);

      db->block[pkt_block_i].header.mcnt[b] = pkt_mcnt;
      db->block[pkt_block_i].header.ant_pair_0[b] = pkt_header.ant0;
      db->block[pkt_block_i].header.ant_pair_1[b] = pkt_header.ant1;
      db->block[pkt_block_i].header.bcnt[b] = pkt_header.bcnt;
      binfo.baselines[pkt_block_i][b] = 1;
    }
    // Update binfo counters for this packet's block
    binfo.flags[pkt_block_i][pkt_offset] = 0;
    binfo.block_packet_counter[pkt_block_i]++;

    return netbcnt;
  } // end "accepted packet" block

  // Else, if the packet is late, but not too late (so we can handle catcher being
  // restarted and bcnt rollover), then ignore it
  else if(pkt_bcnt_dist < 0  && pkt_bcnt_dist > -LATE_PKT_BCNT_THRESHOLD) {
    // Issue warning if not after a reset
    // ARP: would prefer to measure in blocks, not bcnts
    if (binfo.bcnt_start >= binfo.bcnt_log_late) {
       hashpipe_warn("hera_catcher_ibvpkt_thread",
           "Ignoring late packet (%d bcnts late)", -pkt_bcnt_dist);
    }
    return -1;
  } // end "late packet" block

  else {
    // If this is the first out of order packet, issue warning.
    if (binfo.out_of_seq_cnt == 0) {
       hashpipe_warn("hera_catcher_ibvpkt_thread", "out of seq bcnt %012lx (expected: %012lx <= bcnt < %012x)",
           pkt_header.bcnt, binfo.bcnt_start, binfo.bcnt_start+3*BASELINES_PER_BLOCK);
    }

    binfo.out_of_seq_cnt++;

    // If too many out of sequence packets are detected, as can happen when tx
    // is restarted w/o restarting catcher or when a large sequence of packets
    // are missed (e.g. due to network problems), reset first_bcnt and binfo to
    // the new reality using the same databuf blocks that are currently in use.
    if (binfo.out_of_seq_cnt > MAX_OUT_OF_SEQ_PKTS) {
      // To new reality is that pkt_header.bcnt is destined for binfo.block_i
      // (binfo's "current" databuf block).  binfo's bcnt_start field must be
      // changed to reflect the new bcnt range of the current block.  Because
      // the new bcnt_start value will be an arbitrary multiple of
      // BASELINES_PER_BLOCK, we must also update first_bcnt to reflect the
      // (virtual) bcnt of databuf block 0.
      binfo.bcnt_start = pkt_header.bcnt - (pkt_header.bcnt % BASELINES_PER_BLOCK);
      first_bcnt = binfo.bcnt_start - binfo.block_i*BASELINES_PER_BLOCK;
      // Don't log late packets until binfo.bcnt_start advances 2 blocks
      binfo.bcnt_log_late = binfo.bcnt_start + 2*BASELINES_PER_BLOCK;
      // Reset out_of_seq_cnt
      binfo.out_of_seq_cnt = 0;

      hashpipe_warn("hera_catcher_ibvpkt_thread",
      "resetting to first_bcnt %012lx bcnt %012lx block %d based on packet bcnt %012lx",
                     first_bcnt, binfo.bcnt_start, binfo.block_i, pkt_header.bcnt);

      // Reinitialize binfo counter for these blocks
      binfo.block_packet_counter[binfo.block_i] = 0;
      memset(binfo.xeng_pkt_counter[binfo.block_i], 0, N_XENGINES*sizeof(long));
      memset(binfo.flags[binfo.block_i],            1, PACKETS_PER_BLOCK*sizeof(char));
      memset(binfo.baselines[binfo.block_i],        0, BASELINES_PER_BLOCK*sizeof(char));

      int next_block = (binfo.block_i + 1) % CATCHER_N_BLOCKS;
      binfo.block_packet_counter[next_block] = 0;
      memset(binfo.xeng_pkt_counter[next_block], 0, N_XENGINES*sizeof(long));
      memset(binfo.flags[next_block],            1, PACKETS_PER_BLOCK*sizeof(char));
      memset(binfo.baselines[next_block],        0, BASELINES_PER_BLOCK*sizeof(char));

      // Reinitialize the previosuly acquired databuf blocks with new bcnt
      // values.
      initialize_block(db, binfo.bcnt_start);
      initialize_block(db, binfo.bcnt_start+BASELINES_PER_BLOCK);
    }
    return -1;
  }
  return netbcnt;
}

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

static int init(hashpipe_thread_args_t *args)
{
#if 0
    /* Read network params */
    char bindhost[80];
    int bindport = CATCHER_PORT;

    strcpy(bindhost, "0.0.0.0");
#endif
    status_key = args->thread_desc->skey;

    hashpipe_ibvpkt_databuf_t *dbin = (hashpipe_ibvpkt_databuf_t *)args->ibuf;
    const char * thread_name = args->thread_desc->name;
    hashpipe_status_t st = args->st;

    // Verify that the IBVPKTSZ was specified as expected/requried
    if(hashpipe_ibvpkt_databuf_slot_offset(dbin, PKT_OFFSET_HERA_XENG_HEADER) %
            HASHPIPE_IBVPKT_PKT_CHUNK_ALIGNMENT_SIZE != 0
    || hashpipe_ibvpkt_databuf_slot_offset(dbin, PKT_OFFSET_HERA_XENG_PAYLOAD) %
            HASHPIPE_IBVPKT_PKT_CHUNK_ALIGNMENT_SIZE != 0) {
        errno = EINVAL;
        hashpipe_error(thread_name, "IBVPKTSZ!=%d,%d,[...]",
            PKT_OFFSET_HERA_XENG_HEADER, PKT_OFFSET_HERA_XENG_PAYLOAD -
            PKT_OFFSET_HERA_XENG_HEADER);
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
    hputu4(st.buf, "MISSXENG", 0);
    hputu4(st.buf, "MISSEDPK", 0);
    hashpipe_status_unlock_safe(&st);

    fprintf(stdout, "Max offset allowed is set to: %ld\n", MAX_HERA_CATCHER_IDX32);

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
    hera_catcher_bda_input_databuf_t *db = (hera_catcher_bda_input_databuf_t *)args->obuf;
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
    hashpipe_info(thread_name, "using slot_size = %u", slot_size);

    // Force ourself into the hold off state
    fprintf(stdout, "Setting CNETHOLD state to 1. Waiting for someone to set it to 0\n");
    hashpipe_status_lock_safe(&st);
    hputi4(st.buf, "CNETHOLD", 1);
    hputs(st.buf, status_key, "holding");
    hashpipe_status_unlock_safe(&st);

    while(holdoff) {
      sleep(1);
      hashpipe_status_lock_safe(&st);
      // Look for CNETHOLD value
      hgeti4(st.buf, "CNETHOLD", &holdoff);
      if(!holdoff) {
        // Done holding, so delete the key
        hdel(st.buf, "CNETHOLD");
        hputs(st.buf, status_key, "starting");
        fprintf(stdout, "Starting...\n");
      }
      hashpipe_status_unlock_safe(&st);
    }

    // Acquire first two blocks to start
    if(hera_catcher_bda_input_databuf_busywait_free(db, 0) != HASHPIPE_OK){
      if (errno == EINTR){
        // Interrupted by signal, return -1
        hashpipe_error(__FUNCTION__, "interrupted by signal waiting for free databuf");
        pthread_exit(NULL);
      }else{
          hashpipe_error(__FUNCTION__, "error waiting for free databuf");
          pthread_exit(NULL);
      }
    }if(hera_catcher_bda_input_databuf_busywait_free(db, 1) != HASHPIPE_OK){
      if (errno == EINTR){
        // Interrupted by signal, return -1
        hashpipe_error(__FUNCTION__, "interrupted by signal waiting for free databuf");
        pthread_exit(NULL);
      }else{
          hashpipe_error(__FUNCTION__, "error waiting for free databuf");
          pthread_exit(NULL);
      }
    }

    /* Read network params */
    int bindport = CATCHER_PORT;
    // (N_BYTES_PER_PACKET excludes header)
    size_t expected_packet_size = OUTPUT_BYTES_PER_PACKET + sizeof(packet_header_t);

#ifndef TIMING_TEST
    hashpipe_status_lock_safe(&st);
    {
      // Get info from status buffer
      hgeti4(st.buf, "BINDPORT", &bindport);
      hputu4(st.buf, "MISSEDPK", 0);
      hputs(st.buf, status_key, "running");
    }
    hashpipe_status_unlock_safe(&st);
#endif

    /* Main loop */
    int i;
    int rc;
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
    struct timespec start, stop;
    struct timespec recv_start, recv_stop;

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

    fprintf(stdout,"Starting to collect packets..\n");
    fprintf(stdout,"Channels per catcher packet:%ld\n",CHAN_PER_CATCHER_PKT);
    fprintf(stdout,"Packets per block: %ld\n",PACKETS_PER_BLOCK);

    while (run_threads()) {
      // Wait for input block to be filled
      rc = hashpipe_ibvpkt_databuf_wait_filled(dbin, block_idx_in);

      // Packets are not necessarily sent from the X engines continuously so it
      // is possible to have lulls in packet reception that are long enough to
      // cause timeouts in the "wait filled" call.  If/when this happens, we
      // just re-loop so that the `run_threads()` check still gets called every
      // so often during extended lulls.
      if(rc == HASHPIPE_TIMEOUT) {
        // Re-loop
        continue;
      }

      // If we got a non-timeout error, bail out!
      if(rc != HASHPIPE_OK) {
        hashpipe_error(thread_name, "non-timeout error waiting for input block %d", block_idx_in);
        break;
      }

      // Got block, but check for exit request anyway
      if(!run_threads()) {
        // We're outta here! (but first mark the block free)
        hashpipe_ibvpkt_databuf_set_free(dbin, block_idx_in);
        break;
      }

      // Process packets from block
      unsigned char * p_frame = (unsigned char *)hashpipe_ibvpkt_databuf_data(dbin, block_idx_in);
      for(i=0; i < npkts_per_block_in; i++, p_frame += slot_size) {
        packet_count++;

        // Copy packet into any blocks where it belongs.
        const uint64_t bcnt = process_packet((hera_catcher_bda_input_databuf_t *)db, p_frame);

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

        if(bcnt != -1) {
          // Update status
          ns_per_wait = (float)elapsed_wait_ns / packet_count;
          ns_per_recv = (float)elapsed_recv_ns / packet_count;
          ns_per_proc = (float)elapsed_proc_ns / packet_count;

          hashpipe_status_lock_busywait_safe(&st);

          hputu8(st.buf, "NETBCNT", bcnt);
#if 0
          // Gbps = bits_per_packet / ns_per_packet
          // (N_BYTES_PER_PACKET excludes header, so +8 for the header)
          hputr4(st.buf, "NETGBPS", 8*(N_BYTES_PER_PACKET+8)/(ns_per_recv+ns_per_proc));
          hputr4(st.buf, "NETWATNS", ns_per_wait);
          hputr4(st.buf, "NETRECNS", ns_per_recv);
          hputr4(st.buf, "NETPRCNS", ns_per_proc);

          // Get and put min and max values.  The "get-then-put" allows the
          // user to reset the min max values in the status buffer.
          hgeti8(st.buf, "NETWATMN", (long *)&status_ns);
          status_ns = MIN(min_wait_ns, status_ns);
          hputi8(st.buf, "NETWATMN", status_ns);

          hgeti8(st.buf, "NETRECMN", (long *)&status_ns);
          status_ns = MIN(min_recv_ns, status_ns);
          hputi8(st.buf, "NETRECMN", status_ns);

          hgeti8(st.buf, "NETPRCMN", (long *)&status_ns);
          status_ns = MIN(min_proc_ns, status_ns);
          hputi8(st.buf, "NETPRCMN", status_ns);

          hgeti8(st.buf, "NETWATMX", (long *)&status_ns);
          status_ns = MAX(max_wait_ns, status_ns);
          hputi8(st.buf, "NETWATMX", status_ns);

          hgeti8(st.buf, "NETRECMX", (long *)&status_ns);
          status_ns = MAX(max_recv_ns, status_ns);
          hputi8(st.buf, "NETRECMX", status_ns);

          hgeti8(st.buf, "NETPRCMX", (long *)&status_ns);
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

static hashpipe_thread_desc_t hera_catcher_ibvpkt_thread = {
    name: "hera_catcher_ibvpkt_thread",
    skey: "CNETSTAT",
    init: init,
    run:  run,
    ibuf_desc: {hashpipe_ibvpkt_databuf_create},
    obuf_desc: {hera_catcher_bda_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&hera_catcher_ibvpkt_thread);
}

// vi: set ts=8 sw=4 noet :
