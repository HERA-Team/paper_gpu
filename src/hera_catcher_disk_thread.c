/*
 * hera_catcher_disk_thread.c
 *
 * Writes correlated data to disk as metadata hdf5 file + binary data file.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <hdf5.h>
#include <smmintrin.h>
#include <immintrin.h>
#include <hiredis.h>

#include "hashpipe.h"
#include "paper_databuf.h"

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

#ifndef MIN
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

#ifndef MAX
#define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif

#define N_DATA_DIMS (4)
#define N_CHAN_PROCESSED (N_CHAN_TOTAL / (CATCHER_CHAN_SUM_BDA))
#define N_CHAN_RECEIVED (N_CHAN_TOTAL)
#define N_BL_PER_WRITE (32)

#define VERSION_BYTES 32
#define TAG_BYTES 128

// Dummy value to fill all bytes of corr_to_hera_map to indicate that it does
// not yet contain valid data.  For 32 bit ints, this will end up as a
// 0xaaaaaaaa value, which is negative for signed ints.
#define INVALID_INDICATOR (0xaa)

#define MAXSTR 600000

static uint64_t bcnts_per_file;

static hid_t create_hdf5_metadata_file(char * filename)
{
  hid_t status;

  status = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "error creating %s as HDF5 file", filename);
    pthread_exit(NULL);
  }

  return status;
}

static void close_hdf5_metadata_file(hid_t *file_id){
  hid_t status;

  // Close file
  status = H5Fflush(*file_id, H5F_SCOPE_GLOBAL);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to flush file");
    pthread_exit(NULL);
  }
  status = H5Fclose(*file_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close file");
    pthread_exit(NULL);
  }
}

static FILE *open_data_file(char *filename)
{
  FILE *fp = NULL;

  fp = fopen(filename, "wb");

  return fp;
}

static void close_data_file(FILE *fp)
{
  fclose(fp);
}

static void write_metadata(hid_t file_id, uint64_t t0, uint64_t mcnt, double *time_array,
                           int *ant_0_array, int *ant_1_array, double *integration_time,
                           int nblt, char* tag)
{
  hid_t dset_id, dspace_id, status, str_tid;
  uint64_t data;
  hsize_t dspace_dims[] = {nblt};
  char ver[VERSION_BYTES] = GIT_VERSION; // defined at compile time

  // create scalar datasets
  dspace_id = H5Screate(H5S_SCALAR);
  if (dspace_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to create scalar dataspace");
    pthread_exit(NULL);
  }

  // write t0
  dset_id = H5Dcreate(file_id, "t0", H5T_NATIVE_ULONG, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to make t0 dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, H5T_NATIVE_ULONG, H5S_ALL, H5S_ALL, H5P_DEFAULT, &t0);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write t0");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close t0");
    pthread_exit(NULL);
  }

  // write mcnt
  dset_id = H5Dcreate(file_id, "mcnt", H5T_NATIVE_ULONG, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to make mcnt dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, H5T_NATIVE_ULONG, H5S_ALL, H5S_ALL, H5P_DEFAULT, &mcnt);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write mcnt");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close mcnt");
    pthread_exit(NULL);
  }

  // write N_CHAN_PROCESSED
  dset_id = H5Dcreate(file_id, "nfreq", H5T_NATIVE_ULONG, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to make nfreq dataset");
    pthread_exit(NULL);
  }
  data = N_CHAN_PROCESSED;
  status = H5Dwrite(dset_id, H5T_NATIVE_ULONG, H5S_ALL, H5S_ALL, H5P_DEFAULT, &data);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write nfreq");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close nfreq");
    pthread_exit(NULL);
  }

  // write N_STOKES
  dset_id = H5Dcreate(file_id, "nstokes", H5T_NATIVE_ULONG, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to make nstokes dataset");
    pthread_exit(NULL);
  }
  data = N_STOKES;
  status = H5Dwrite(dset_id, H5T_NATIVE_ULONG, H5S_ALL, H5S_ALL, H5P_DEFAULT, &data);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write nstokes");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close nstokes");
    pthread_exit(NULL);
  }

  // version
  str_tid = H5Tcopy(H5T_C_S1);
  H5Tset_size(str_tid, VERSION_BYTES);
  dset_id = H5Dcreate(file_id, "corr_ver", str_tid, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to make corr_ver dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, str_tid, H5S_ALL, H5S_ALL, H5P_DEFAULT, ver);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write corr_ver");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close corr_ver");
    pthread_exit(NULL);
  }

  // tag
  str_tid = H5Tcopy(H5T_C_S1);
  H5Tset_size(str_tid, TAG_BYTES);
  dset_id = H5Dcreate(file_id, "tag", str_tid, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to make tag dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, str_tid, H5S_ALL, H5S_ALL, H5P_DEFAULT, tag);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write tag");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close tag");
    pthread_exit(NULL);
  }
  status = H5Sclose(dspace_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close dataspace");
    pthread_exit(NULL);
  }

  // create new dspace
  dspace_id = H5Screate_simple(1, dspace_dims, NULL);

  // write ant_0_array
  dset_id = H5Dcreate(file_id, "ant_0_array", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to create ant_0_array dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, ant_0_array);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write ant_0_array data");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close ant_0_array dataset");
    pthread_exit(NULL);
  }

  // write ant_1_array
  dset_id = H5Dcreate(file_id, "ant_1_array", H5T_NATIVE_INT, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to create ant_1_array dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, ant_1_array);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write ant_1_array data");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close ant_1_array dataset");
    pthread_exit(NULL);
  }

  // write time_array
  dset_id = H5Dcreate(file_id, "time_array", H5T_NATIVE_DOUBLE, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to create time_array dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, time_array);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write time_array data");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close time_array dataset");
    pthread_exit(NULL);
  }

  // write integration_time
  dset_id = H5Dcreate(file_id, "integration_time", H5T_NATIVE_DOUBLE, dspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  if (dset_id < 0) {
    hashpipe_error(__FUNCTION__, "Failed to create integration_time dataset");
    pthread_exit(NULL);
  }
  status = H5Dwrite(dset_id, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, integration_time);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to write integration_time data");
    pthread_exit(NULL);
  }
  status = H5Dclose(dset_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close integration_time dataset");
    pthread_exit(NULL);
  }

  // clean up
  status = H5Sclose(dspace_id);
  if (status < 0) {
    hashpipe_error(__FUNCTION__, "Failed to close dataspace");
    pthread_exit(NULL);
  }
}

// The data in the files should be indexed in real antennas numbers, not
// in correlator numbers. Get the corr_to_hera map from redis to get right labelling.
/* Read the correlator to hera_antennas map from redis via the
   corr:corr_to_hera_map key */
static void get_corr_to_hera_map(redisContext *c, int *corr_to_hera_map) {
  char redis_mapping[MAXSTR];
  char *line;
  char *saveptr = NULL;
  redisReply *reply;
  int iant, antnum;

  // read mapping from redis
  redis_mapping[0] = EOF;
  reply = redisCommand(c, "HGET corr corr_to_hera_map");
  if (c->err) {
    printf("HGET error: %s\n", c->errstr);
    pthread_exit(NULL);
  }

  // copy to new buffer
  strcpy(redis_mapping, reply->str);

  if (redis_mapping[0] == EOF){
    printf("Cannot read configuration from redis.\n");
    pthread_exit(NULL);
  }

  line = strtok_r(redis_mapping, "\n", &saveptr);
  iant = 0;
  while (line != NULL) {
    sscanf(line, "%d", &antnum);
    line = strtok_r(NULL, "\n", &saveptr);
    corr_to_hera_map[iant] = antnum;
    iant+=1;
    if (iant > N_ANTS) {
      printf("More ants in config than correlator supports.\n");
      pthread_exit(NULL);
    }
  }

  // clean up
  freeReplyObject(reply);
}

/* Get the integration time for each baseline from redis (set by config file) */
static void get_integration_time(redisContext *c, double *integration_time_buf, uint32_t acc_len) {
  char int_bin_str[MAXSTR];
  char *line;
  char *saveptr = NULL;
  redisReply *reply;
  int i;
  double inttime;

  // read mapping from redis
  int_bin_str[0] = EOF;
  reply = redisCommand(c, "HGET corr integration_bin");
  if (c->err) {
    printf("HGET error: %s\n", c->errstr);
    pthread_exit(NULL);
  }

  // copy to new buffer
  strcpy(int_bin_str, reply->str);

  if (int_bin_str[0] == EOF){
    printf("Cannot read configuration from redis.\n");
    pthread_exit(NULL);
  }

  line = strtok_r(int_bin_str, "\n", &saveptr);
  i = 0;
  while (line != NULL) {
    sscanf(line, "%lf", &inttime);
    line = strtok_r(NULL, "\n", &saveptr);
    integration_time_buf[i] = inttime;
    i+=1;
  }

  // clean up
  freeReplyObject(reply);

  // finish computing integration time
  for(i=0; i< bcnts_per_file; i++){
    integration_time_buf[i] *= acc_len * TIME_DEMUX * 2L * N_CHAN_TOTAL_GENERATED/(double)FENG_SAMPLE_RATE;
  }
}

/*
Turn an mcnt into a UNIX time in double-precision.
*/
static double mcnt2time(uint64_t mcnt, uint64_t sync_time_ms)
{
    return (sync_time_ms / 1000.) + (mcnt * (2L * N_CHAN_TOTAL_GENERATED / (double)FENG_SAMPLE_RATE));
}

/*
 *  Compute JD for the given gps time

static double unix2julian(double unixtime)
{
    return (2440587.5 + (unixtime / (double)(86400.0)));
}
*/

static double compute_jd_from_mcnt(uint64_t mcnt, uint64_t sync_time_ms, double integration_time)
{
   double unix_time = (sync_time_ms / 1000.) + (mcnt * (2L * N_CHAN_TOTAL_GENERATED / (double)FENG_SAMPLE_RATE));
   unix_time = unix_time - integration_time/2;

   return (2440587.5 + (unix_time / (double)(86400.0)));
}

/*
 * Write N_BL_PER_WRITE bcnts to the dataset
 */
static void write_baseline_index(FILE *fstream, size_t nblts, uint64_t *visdata_buf)
{
  // 8 bytes per element (4 bytes each real + imaginary)
  size_t nelem = 8 * nblts * N_CHAN_PROCESSED * N_STOKES;

  // write to open file
  fwrite(visdata_buf, nelem, 1, fstream);
}

// Get the even-sample / first-pol / first-complexity of the correlation buffer for chan `c` baseline `b`
static void compute_sum_diff(int32_t *in, int32_t *out_sum, int32_t *out_diff, uint32_t bl) {

    int xchan, chan, bcnt, offset;

    //Buffers for a single full-stokes baseline
    // 256 bits = 4 stokes * 2 real/imag * 32 bits == 1 channel

    #if CATCHER_CHAN_SUM_BDA != 1
      int c;
      __m256i sum_even = _mm256_set_epi64x(0ULL,0ULL,0ULL,0ULL);
      __m256i sum_odd  = _mm256_set_epi64x(0ULL,0ULL,0ULL,0ULL);
    #endif

    __m256i val_even = _mm256_set_epi64x(0ULL,0ULL,0ULL,0ULL);
    __m256i val_odd  = _mm256_set_epi64x(0ULL,0ULL,0ULL,0ULL);

    __m256i *in_even256, *in_odd256;
    __m256i *out_sum256  = (__m256i *)out_sum;
    __m256i *out_diff256 = (__m256i *)out_diff;

    for(bcnt=0; bcnt<N_BL_PER_WRITE; bcnt++){
       offset = hera_catcher_bda_input_databuf_by_bcnt_idx32(bcnt+bl, 0);
       in_even256 = (__m256i *)(in + offset);
       offset = hera_catcher_bda_input_databuf_by_bcnt_idx32(bcnt+bl, 1);
       in_odd256  = (__m256i *)(in + offset);

       for(xchan=0; xchan< N_CHAN_TOTAL; xchan+= CATCHER_CHAN_SUM_BDA){
          chan = xchan/CATCHER_CHAN_SUM_BDA;

          #if CATCHER_CHAN_SUM_BDA != 1
             // Add channels
             for(c=0; c<CATCHER_CHAN_SUM_BDA; c++){
                val_even = _mm256_load_si256(in_even256 + xchan + c);
                val_odd  = _mm256_load_si256(in_odd256  + xchan + c);
                if (c==0){
                   sum_even = val_even;
                   sum_odd  = val_odd;
                }
                else{
                   sum_even = _mm256_add_epi32(sum_even, val_even);
                   sum_odd  = _mm256_add_epi32(sum_odd,  val_odd);
                }
             }
             // Write to output
             _mm256_store_si256((out_sum256  + (bcnt*N_CHAN_PROCESSED + chan)), _mm256_add_epi32(sum_even, sum_odd));
             _mm256_store_si256((out_diff256 + (bcnt*N_CHAN_PROCESSED + chan)), _mm256_sub_epi32(sum_even, sum_odd));

          #else
             // Load and write sum/diff
             val_even = _mm256_load_si256(in_even256 + xchan);
             val_odd  = _mm256_load_si256(in_odd256 + xchan);
             _mm256_store_si256((out_sum256  + (bcnt*N_CHAN_PROCESSED+ chan)), _mm256_add_epi32(val_even, val_odd));
             _mm256_store_si256((out_diff256 + (bcnt*N_CHAN_PROCESSED+ chan)), _mm256_sub_epi32(val_even, val_odd));
          #endif
       }
    }
return;
}

static int init(hashpipe_thread_args_t *args)
{
    fprintf(stdout, "Initializing Catcher disk thread\n");

    return 0;
}

static void *run(hashpipe_thread_args_t * args)
{
    // Local aliases to shorten access to args fields
    // Our input buffer is a hera_catcher_bda_input_databuf
    hera_catcher_bda_input_databuf_t *db_in = (hera_catcher_bda_input_databuf_t *)args->ibuf;
    hera_catcher_autocorr_databuf_t *db_out = (hera_catcher_autocorr_databuf_t *)args->obuf;
    hashpipe_status_t st = args->st;
    const char * status_key = args->thread_desc->skey;

    // Timers for performance monitoring
    struct timespec t_start, t_stop, w_start, w_stop;
    float gbps, min_gbps;

    struct timespec start, finish;
    clock_gettime(CLOCK_MONOTONIC, &finish);
    uint64_t t_ns;
    uint64_t w_ns;
    uint64_t min_t_ns = 999999999;
    uint64_t min_w_ns = 999999999;
    uint64_t max_t_ns = 0;
    uint64_t max_w_ns = 0;
    uint64_t elapsed_t_ns = 0;
    uint64_t elapsed_w_ns = 0;
    float bl_t_ns = 0.0;
    float bl_w_ns = 0.0;

    // Buffers for file name strings
    char hdf5_meta_fname[128];
    char sum_fname[128];
    char diff_fname[128];
    char data_directory[128];
    char cwd[128];

    // Variables for sync time and computed gps time / JD
    uint64_t sync_time_ms = 0;
    double gps_time;
    double julian_time;
    int int_jd;

    // Variables for data collection parameters
    uint32_t acc_len;
    uint32_t nfiles = 1;
    uint32_t file_cnt = 0;
    uint32_t trigger = 0;
    char tag[TAG_BYTES];
    uint64_t baseline_dist[N_BDABUF_BINS];
    uint64_t Nants = 0;
    int corr_to_hera_map[N_ANTS];
    memset(corr_to_hera_map, INVALID_INDICATOR, N_ANTS*sizeof(int));

    // Init status variables
    hashpipe_status_lock_safe(&st);
    hputi8(st.buf, "DISKMCNT", 0);
    hputu4(st.buf, "TRIGGER", trigger);
    hputu4(st.buf, "NDONEFIL", file_cnt);
    hashpipe_status_unlock_safe(&st);

    // Redis connection
    redisContext *c;
    int use_redis = 1;
    int idle = 0;

    struct timeval redistimeout = { 0, 100000 }; // 0.1 seconds
    c = redisConnectWithTimeout(REDISHOST, REDISPORT, redistimeout);
    if (c == NULL || c->err) {
        if (c) {
            fprintf(stderr, "Redis connection error: %s\n", c->errstr);
            redisFree(c);
            use_redis = 0;
        } else {
            fprintf(stderr, "Connection error: can't allocate redis context\n");
            use_redis = 0;
        }
    }
    // XXX code is inconsistent wrt "use_redis". Either respect it or raise error

    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        redisCommand(c, "HMSET corr:catcher cwd %s", cwd);
    } else {
        fprintf(stderr, "Failed to find catcher_cwd to set it in redis\n");
    }

    // Indicate via redis that we've started but not taking data
    redisCommand(c, "HMSET corr:is_taking_data state False time %d", (int)time(NULL));
    redisCommand(c, "EXPIRE corr:is_taking_data 60");
    redisCommand(c, "HMSET corr:current_file filename NONE time %d", (int)time(NULL));

    /* Loop(s) */
    int32_t *db_in32;
    hera_catcher_bda_input_header_t header;
    int rv;
    int curblock_in=0;
    int curblock_out = 0;
    double file_start_t, file_stop_t, file_duration; // time from bcnt
    int64_t file_nblts=0;
    int32_t curr_file_bcnt = -1;
    uint32_t bctr, strt_bcnt, stop_bcnt, break_bcnt;        // bcnt variable
    int b;
    unsigned int nbls, block_offset;
    uint32_t file_offset;
    uint32_t offset_in, offset_out; // for autocorrs
    int auto_ants_filled = 0;
    uint16_t ant;

    // files
    hid_t meta_fid;
    FILE *sum_file;
    #ifndef SKIP_DIFF
    FILE *diff_file;
    #endif

    // aligned_alloc because we're going to use 256-bit AVX instructions
    int32_t *bl_buf_sum  = (int32_t *)aligned_alloc(32, N_BL_PER_WRITE * N_CHAN_PROCESSED * N_STOKES * 2 * sizeof(int32_t));
    int32_t *bl_buf_diff = (int32_t *)aligned_alloc(32, N_BL_PER_WRITE * N_CHAN_PROCESSED * N_STOKES * 2 * sizeof(int32_t));

    memset(bl_buf_sum,  0, N_BL_PER_WRITE * N_CHAN_PROCESSED * N_STOKES * 2 * sizeof(int32_t));
    memset(bl_buf_diff, 0, N_BL_PER_WRITE * N_CHAN_PROCESSED * N_STOKES * 2 * sizeof(int32_t));

    // Init here, realloc after reading baseline distribution from sharedmem
    double *integration_time_buf = (double *)malloc(1 * sizeof(double));
    double *time_array_buf       = (double *)malloc(1 * sizeof(double));
    int *ant_0_array             =    (int *)malloc(1 * sizeof(int));
    int *ant_1_array             =    (int *)malloc(1 * sizeof(int));

    while (run_threads()) {
        // Note waiting status,
        hashpipe_status_lock_safe(&st);
        if (idle) {
            hputs(st.buf, status_key, "idle");
        } else {
            hputs(st.buf, status_key, "waiting");
        }
        hashpipe_status_unlock_safe(&st);

        // Expire the "corr:is_taking_data" key after 60 seconds.
        // If this pipeline goes down, we will know because the key will disappear
        redisCommand(c, "EXPIRE corr:is_taking_data 60");

        // Wait for new input block to be filled
        while ((rv=hera_catcher_bda_input_databuf_wait_filled(db_in, curblock_in)) != HASHPIPE_OK) {
            if (rv==HASHPIPE_TIMEOUT) {
                hashpipe_status_lock_safe(&st);
                hputs(st.buf, status_key, "blocked_in");
                hashpipe_status_unlock_safe(&st);
                continue;
            } else {
                hashpipe_error(__FUNCTION__, "error waiting for filled databuf");
                pthread_exit(NULL);
                break;
            }
        }

        db_in32 = (int32_t *)db_in->block[curblock_in].data;
        header = db_in->block[curblock_in].header;

        // Got a new data block, update status
        hashpipe_status_lock_safe(&st);
        hputs(st.buf, status_key, "writing");
        hputi4(st.buf, "DISKBKIN", curblock_in);
        hputu8(st.buf, "DISKMCNT", header.mcnt[0]);
        hputu8(st.buf, "DISKBCNT", header.bcnt[0]);
        hgetu8(st.buf, "BDANANT", &Nants);
        hashpipe_status_unlock_safe(&st);

        /* Copy auto correlations to autocorr buffer iif Nants and corr_to_hera_map are valid */
        if (Nants > 0 && (corr_to_hera_map[0] & 0xff000000) != (INVALID_INDICATOR<<24)) {
           if (auto_ants_filled == 0){
              // Wait for next buffer to get free
              while ((rv= hera_catcher_autocorr_databuf_busywait_free(db_out, curblock_out)) != HASHPIPE_OK) {
                  if (rv==HASHPIPE_TIMEOUT) {
                      hashpipe_status_lock_safe(&st);
                      hputs(st.buf, status_key, "blocked redis thread");
                      hashpipe_status_unlock_safe(&st);
                      continue;
                  } else {
                      hashpipe_error(__FUNCTION__, "error waiting for free databuf");
                      pthread_exit(NULL);
                      break;
                  }
              }
              // Clear all ant flags
              memset(db_out->block[curblock_out].header.ant, 0, sizeof(db_out->block[curblock_out].header.ant));
           }

           for (bctr=0; bctr < BASELINES_PER_BLOCK; bctr++){
               // Autocorr blocks are indexed by antennas numbers (not corr numbers)
               ant = corr_to_hera_map[header.ant_pair_0[bctr]];
               if(ant > N_ANTS_TOTAL-1) {
                  // Should "never" happen so don't worry about throttling this message
                  hashpipe_warn(__FUNCTION__, "antenna number %u exceeds N_ANTS_TOTAL-1 %d", ant, (N_ANTS_TOTAL-1));
               }
               if((header.ant_pair_0[bctr] == header.ant_pair_1[bctr]) && (db_out->block[curblock_out].header.ant[ant]==0)){
                  offset_in = hera_catcher_bda_input_databuf_by_bcnt_idx32(bctr, 0);
                  offset_out = hera_catcher_autocorr_databuf_idx32(ant);
                  memcpy((db_out->block[curblock_out].data + offset_out), (db_in32 + offset_in), N_CHAN_TOTAL*N_STOKES*2*sizeof(uint32_t));
                  auto_ants_filled++;
                  db_out->block[curblock_out].header.ant[ant] = 1;
               }
           }

           // If you have autocorrs of all antennas
           // Mark output block as full and advance
           if (auto_ants_filled >= Nants){
              // Update databuf headers
              db_out->block[curblock_out].header.num_ants = Nants;
              db_out->block[curblock_out].header.julian_time = compute_jd_from_mcnt(header.mcnt[bctr-1], sync_time_ms, 2);
              if (hera_catcher_autocorr_databuf_set_filled(db_out, curblock_out) != HASHPIPE_OK) {
                 hashpipe_error(__FUNCTION__, "error marking out databuf %d full", curblock_out);
                 pthread_exit(NULL);
              }
              curblock_out = (curblock_out + 1) % AUTOCORR_N_BLOCKS;
              auto_ants_filled = 0;
           }
        }

        // reset elapsed time counters
        elapsed_w_ns = 0.0;
        elapsed_t_ns = 0.0;

        // Get time that F-engines were last sync'd
	hashpipe_status_lock_safe(&st);
        hgetu8(st.buf, "SYNCTIME", &sync_time_ms);

        // Get the integration time reported by the correlator
        hgetu4(st.buf, "INTTIME", &acc_len);

        // Get the number of files to write
        hgetu4(st.buf, "NFILES", &nfiles);
        // Update the status with how many files we've already written
        hputu4(st.buf, "NDONEFIL", file_cnt);

        // Data tag
        hgets(st.buf, "TAG", TAG_BYTES, tag);

        // Wait for the trigger to write files
        hgetu4(st.buf, "TRIGGER", &trigger);
        hashpipe_status_unlock_safe(&st);

        // If we have written all the files we were commanded to
        // start marking blocks as done and idling until a new
        // trigger is received
        if (trigger) {
          fprintf(stdout, "Catcher got a new trigger and will write %d files\n", nfiles);
          file_cnt = 0;
          hashpipe_status_lock_safe(&st);
          hputu4(st.buf, "TRIGGER", 0);
          hputu4(st.buf, "NDONEFIL", file_cnt);

          // Get baseline distribution from sharedmem -- this has to be done here
          // to ensure that redis database is updated before reading.
          hgetu8(st.buf,"BDANANT", &Nants);
          hgetu8(st.buf,"NBL2SEC", &baseline_dist[0]);
          hgetu8(st.buf,"NBL4SEC", &baseline_dist[1]);
          hgetu8(st.buf,"NBL8SEC", &baseline_dist[2]);
          hgetu8(st.buf,"NBL16SEC",&baseline_dist[3]);
          hashpipe_status_unlock_safe(&st);

          bcnts_per_file = 8*baseline_dist[0] + 4*baseline_dist[1] + 2*baseline_dist[2] + baseline_dist[3];
          fprintf(stdout,"Baseline Distribution per file:\n");
          fprintf(stdout,"8 x %ld\t 4 x %ld\t 2 x %ld\t 1 x %ld\n",
                  baseline_dist[0],baseline_dist[1],baseline_dist[2],baseline_dist[3]);
          fprintf(stdout,"Total Baselines: %ld\n", bcnts_per_file);

          fprintf(stdout, "N_CHAN_PROCESSED: %d\n", N_CHAN_PROCESSED);
          fprintf(stdout, "CATCHER_CHAN_SUM_BDA: %d\n", CATCHER_CHAN_SUM_BDA);

          integration_time_buf = (double *)realloc(integration_time_buf, bcnts_per_file * sizeof(double));
          time_array_buf       = (double *)realloc(time_array_buf,       bcnts_per_file * sizeof(double));
          ant_0_array          =    (int *)realloc(ant_0_array,          bcnts_per_file * sizeof(int));
          ant_1_array          =    (int *)realloc(ant_1_array,          bcnts_per_file * sizeof(int));

          idle = 0;
          if (use_redis) {
              // Create the "corr:is_taking_data" hash. This will be set to
              // state=False when data taking is complete. Or if this pipeline
              // exits the key will expire.
              redisCommand(c, "HMSET corr:is_taking_data state True time %d", (int)time(NULL));
              redisCommand(c, "EXPIRE corr:is_taking_data 60");
          }

        } else if (file_cnt >= nfiles || idle) {
          // If we're transitioning to idle state
          // Indicate via redis that we're no longer taking data
          if (!idle) {
              redisCommand(c, "HMSET corr:is_taking_data state False time %d", (int)time(NULL));
              redisCommand(c, "EXPIRE corr:is_taking_data 60");
          }
          idle = 1;
          // Mark input block as free and advance
          if(hera_catcher_bda_input_databuf_set_free(db_in, curblock_in) != HASHPIPE_OK) {
              hashpipe_error(__FUNCTION__, "error marking databuf %d free", curblock_in);
              pthread_exit(NULL);
          }
          if (use_redis) {
            // Let RTP know we have a new session available
            redisCommand(c, "HMSET rtp:has_new_data state True");
          }
          //printf("disk_thread: curblock_in=%d -> %d\n", curblock_in, (curblock_in + 1) % CATCHER_N_BLOCKS);
          curblock_in = (curblock_in + 1) % CATCHER_N_BLOCKS;
          continue;
        }

        // If we make it to here we're not idle any more.
        // Usually this would mean there has been another trigger but
        // it could be some weirdness where someone tried to take more
        // data by incrementing NFILES without retriggering.
        idle = 0;

        // Start writing files!
        // A file is defined as bcnts_per_file number of bcnts. If a bcnt belonging
        // to a new intergation arrives, close the old file and start a new file.

        clock_gettime(CLOCK_MONOTONIC, &start);

        for (bctr=0 ; bctr< BASELINES_PER_BLOCK; bctr += N_BL_PER_WRITE){

          // We write N_BL_PER_WRITE at a time.
          // these variables store the baseline numbers for the start and end of these blocks
          strt_bcnt = header.bcnt[bctr];
          stop_bcnt = header.bcnt[bctr+N_BL_PER_WRITE-1];

          clock_gettime(CLOCK_MONOTONIC, &t_start);
          compute_sum_diff(db_in32, bl_buf_sum, bl_buf_diff, bctr);
          clock_gettime(CLOCK_MONOTONIC, &t_stop);

          t_ns = ELAPSED_NS(t_start, t_stop);
          elapsed_t_ns += t_ns;
          min_t_ns = MIN(t_ns, min_t_ns);
          max_t_ns = MAX(t_ns, max_t_ns);

          // If the start and end of this block belong in the same file AND
          // The start of this block is not the start of a new file...
          if (((strt_bcnt / bcnts_per_file) == (stop_bcnt / bcnts_per_file)) &&
               (strt_bcnt % bcnts_per_file != 0)){

             // If there is a file already open...
             // Copy all contents
             if (curr_file_bcnt >= 0){

                file_offset = strt_bcnt - curr_file_bcnt;

                clock_gettime(CLOCK_MONOTONIC, &w_start);

                write_baseline_index(sum_file, N_BL_PER_WRITE, (uint64_t *)bl_buf_sum);
                #ifndef SKIP_DIFF
                write_baseline_index(diff_file, N_BL_PER_WRITE, (uint64_t *)bl_buf_diff);
                #endif

                clock_gettime(CLOCK_MONOTONIC, &w_stop);

                for(b=0; b< N_BL_PER_WRITE; b++){
                   ant_0_array[file_offset+b]    = corr_to_hera_map[header.ant_pair_0[bctr+b]];
                   ant_1_array[file_offset+b]    = corr_to_hera_map[header.ant_pair_1[bctr+b]];

                   time_array_buf[file_offset+b] = compute_jd_from_mcnt(header.mcnt[bctr+b], sync_time_ms,
                                                   integration_time_buf[file_offset+b]);
                }

                file_nblts += N_BL_PER_WRITE;

                w_ns = ELAPSED_NS(w_start, w_stop);
                elapsed_w_ns += w_ns;
                min_w_ns = MIN(w_ns, min_w_ns);
                max_w_ns = MAX(w_ns, max_w_ns);
             }
          } else {
             // the block has a file boundary OR this block starts with a new file.

             // Calculate the bcnt where we need to start a new file.
             // This block might start at a new file. Otherwise
             // We need a new file at the next bcnts_per_file boundary.
             if (strt_bcnt % bcnts_per_file == 0){
	       break_bcnt = strt_bcnt;
             } else {
	       break_bcnt = ((strt_bcnt / bcnts_per_file) + 1) * bcnts_per_file;
             }

             // If there is an open file, copy the relevant part of the block
             // and close the file. Open a new file for the rest of the block.
             if (curr_file_bcnt >=0){
                 // copy data
                 nbls = break_bcnt - strt_bcnt;

                 if (nbls > 0){
                    file_offset = strt_bcnt - curr_file_bcnt;

                    clock_gettime(CLOCK_MONOTONIC, &w_start);
                    write_baseline_index(sum_file, nbls, (uint64_t *)bl_buf_sum);
                    #ifndef SKIP_DIFF
                    write_baseline_index(diff_file, nbls, (uint64_t *)bl_buf_diff);
                    #endif

                    clock_gettime(CLOCK_MONOTONIC, &w_stop);

                    for(b=0; b< nbls; b++){
                       ant_0_array[file_offset+b]    = corr_to_hera_map[header.ant_pair_0[bctr+b]];
                       ant_1_array[file_offset+b]    = corr_to_hera_map[header.ant_pair_1[bctr+b]];
                       time_array_buf[file_offset+b] = compute_jd_from_mcnt(header.mcnt[bctr+b], sync_time_ms,
                                                       integration_time_buf[file_offset+b]);
                    }
                    file_nblts += nbls;

                    w_ns = ELAPSED_NS(w_start, w_stop);
                    elapsed_w_ns += w_ns;
                    min_w_ns = MIN(w_ns, min_w_ns);
                    max_w_ns = MAX(w_ns, max_w_ns);
                 }

                 // finish meta data and close the file
                 gps_time = mcnt2time(header.mcnt[bctr+nbls], sync_time_ms);
                 file_stop_t = gps_time;
                 file_duration = file_stop_t - file_start_t;

                 write_metadata(meta_fid, sync_time_ms, header.mcnt[bctr+nbls],
                                time_array_buf, ant_0_array, ant_1_array, integration_time_buf,
                                file_nblts, tag);
                 close_hdf5_metadata_file(&meta_fid);
                 close_data_file(sum_file);

                 #ifndef SKIP_DIFF
                 close_data_file(diff_file);
                 #endif

                 file_cnt += 1;

                 if (use_redis) {
                   redisCommand(c, "RPUSH corr:files:raw %s", sum_fname);
                   #ifndef SKIP_DIFF
                   redisCommand(c, "RPUSH corr:files:raw %s", diff_fname);
                   #endif
                 }

                 // add file to M&C
                 /* strcpy(hdf5_mc_fname, hdf5_sum_fname); */
                 /* rc = pthread_create(&thread_id, NULL, add_mc_obs_pthread, hdf5_mc_fname); */
                 /* if (rc) { */
                 /*   fprintf(stderr, "Error launching M&C thread\n"); */
                 /* } */

                 hashpipe_status_lock_safe(&st);
                 hputr4(st.buf, "FILESEC", file_duration);
                 hputi8(st.buf, "NDONEFIL", file_cnt);
                 hashpipe_status_unlock_safe(&st);

                 // If this is the last file, mark this block done and get out of the loop
                 if (file_cnt >= nfiles) {
                     fprintf(stdout, "Catcher has written %d file and is going to sleep\n", file_cnt);
                     curr_file_bcnt = -1; //So the next trigger will start a new file
                     break;
                 }
             }

             // Open new sum and difference files
             // Init all counters to zero
             file_nblts = 0;
             memset(ant_0_array,          0, bcnts_per_file * sizeof(uint16_t));
             memset(ant_1_array,          0, bcnts_per_file * sizeof(uint16_t));
             memset(time_array_buf,       0, bcnts_per_file * sizeof(double));

             curr_file_bcnt = break_bcnt;
             block_offset = bctr + break_bcnt - strt_bcnt;
             fprintf(stdout, "Curr file bcnt: %d\n", curr_file_bcnt);
             fprintf(stdout, "Curr file mcnt: %ld\n", header.mcnt[block_offset]);
             gps_time = mcnt2time(header.mcnt[block_offset], sync_time_ms);
             julian_time = 2440587.5 + (gps_time / (double)(86400.0));
             file_start_t = gps_time;

             // Make a new folder for output
             if (file_cnt == 0) {
               int_jd = (int)julian_time;
               sprintf(data_directory, "%d", int_jd);
               fprintf(stdout, "Making directory %s\n", data_directory);
               mkdir(data_directory, 0777);
               chmod(data_directory, 0777);
             }

             sprintf(sum_fname, "%d/zen.%7.5lf.sum.dat", int_jd, julian_time);
             sprintf(hdf5_meta_fname, "%d/zen.%7.5lf.meta.hdf5", int_jd, julian_time);
             fprintf(stdout, "Opening new file %s\n", sum_fname);
             meta_fid = create_hdf5_metadata_file(hdf5_meta_fname);
             sum_file = open_data_file(sum_fname);
             if (use_redis) {
               redisCommand(c, "HMSET corr:current_file filename %s time %d", sum_fname, (int)time(NULL));
             }

             #ifndef SKIP_DIFF
               sprintf(diff_fname, "%d/zen.%7.5lf.diff.dat", int_jd, julian_time);
               fprintf(stdout, "Opening new file %s\n", diff_fname);
               diff_file = open_data_file(diff_fname);
             #endif

             // Get the antenna positions and baseline orders
             // These are needed for populating the ant_[1|2]_array and uvw_array
             get_corr_to_hera_map(c, corr_to_hera_map);
             get_integration_time(c, integration_time_buf, acc_len);

             // Copy data to the right location
             nbls = stop_bcnt - break_bcnt + 1;

             if (nbls > 0){
                file_offset = break_bcnt - curr_file_bcnt;

                for(b=0; b< nbls; b++){
		  ant_0_array[file_offset+b]    = corr_to_hera_map[header.ant_pair_0[block_offset+b]];
		  ant_1_array[file_offset+b]    = corr_to_hera_map[header.ant_pair_1[block_offset+b]];
		  time_array_buf[file_offset+b] = compute_jd_from_mcnt(header.mcnt[block_offset+b], sync_time_ms,
								       integration_time_buf[file_offset+b]);
                }

                clock_gettime(CLOCK_MONOTONIC, &w_start);
		// have to skip over previously written content in bl_buf_sum/diff
                write_baseline_index(sum_file, nbls, (uint64_t *)(bl_buf_sum+2*(break_bcnt - strt_bcnt)*N_CHAN_PROCESSED*N_STOKES));
                #ifndef SKIP_DIFF
                write_baseline_index(diff_file, nbls, (uint64_t *)(bl_buf_diff+2*(break_bcnt - strt_bcnt)*N_CHAN_PROCESSED*N_STOKES));
                #endif
                clock_gettime(CLOCK_MONOTONIC, &w_stop);

                file_nblts += nbls;

                w_ns = ELAPSED_NS(w_start, w_stop);
                elapsed_w_ns += w_ns;
                min_w_ns = MIN(w_ns, min_w_ns);
                max_w_ns = MAX(w_ns, max_w_ns);
             }
          }
        }

        clock_gettime(CLOCK_MONOTONIC, &finish);

        // Compute processing time for this block
        bl_t_ns = (float)elapsed_t_ns / BASELINES_PER_BLOCK;
        bl_w_ns = (float)elapsed_w_ns / BASELINES_PER_BLOCK;

        hashpipe_status_lock_safe(&st);
        hputr4(st.buf, "DISKTBNS", bl_t_ns);
        hputi8(st.buf, "DISKTMIN", min_t_ns);
        hputi8(st.buf, "DISKTMAX", max_t_ns);
        hputr4(st.buf, "DISKWBNS", bl_w_ns);
        hputi8(st.buf, "DISKWMIN", min_w_ns);
        hputi8(st.buf, "DISKWMAX", max_w_ns);

        hputi8(st.buf, "DISKWBL", w_ns/BASELINES_PER_BLOCK);

        hgetr4(st.buf, "DISKMING", &min_gbps);
        #ifndef SKIP_DIFF
          gbps = (float)(2 * BASELINES_PER_BLOCK*N_CHAN_PROCESSED*N_STOKES*64L)/ELAPSED_NS(start,finish);
        #else
          gbps = (float)(BASELINES_PER_BLOCK*N_CHAN_PROCESSED*N_STOKES*64L)/ELAPSED_NS(start,finish);
        #endif
        hputr4(st.buf, "DISKGBPS", gbps);
        hputr4(st.buf, "DUMPMS", ELAPSED_NS(start,finish) / 1000000.0);
        if(min_gbps == 0 || gbps < min_gbps) {
          hputr4(st.buf, "DISKMING", gbps);
        }
        hashpipe_status_unlock_safe(&st);

        // Mark input block as free and advance
        if(hera_catcher_bda_input_databuf_set_free(db_in, curblock_in) != HASHPIPE_OK) {
            hashpipe_error(__FUNCTION__, "error marking databuf %d free", curblock_in);
            pthread_exit(NULL);
        }
        //fprintf(stdout, "disk_thread: curblock_in=%d->%d\n", curblock_in, (curblock_in + 1) % CATCHER_N_BLOCKS);
        curblock_in = (curblock_in + 1) % CATCHER_N_BLOCKS;

        /* Check for cancel */
        pthread_testcancel();
    }

    // Thread success!
    return NULL;
}

static hashpipe_thread_desc_t hera_catcher_disk_thread = {
    name: "hera_catcher_disk_thread",
    skey: "DISKSTAT",
    init: init,
    run:  run,
    ibuf_desc: {hera_catcher_bda_input_databuf_create},
    obuf_desc: {hera_catcher_autocorr_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&hera_catcher_disk_thread);
}
