#ifndef __CONFIGURE_HH__
#define __CONFIGURE_HH__
#include <ctime>  //time_t
#include <sys/types.h> // off_t


#define PFS_BLOCK_SIZE 1 // 1 Kilobyte
#define STRIP_SIZE 4     // 4 blocks
#define NUM_FILE_SERVERS 5
#define CLIENT_CACHE_SIZE 2 // 2 Megabytes

struct pfs_stat {
  time_t pst_mtime; /* time of last data modification */
  time_t pst_ctime; /* time of creation */
  off_t pst_size;    /* File size in bytes */
};



// range type [ ), also known as "0-based coordinate" system, for the convenient of intersect and union algorithm
// include the first but does not include the last, so to represent a empty range, it will be [X,X)
// more detail, please refer to google

#endif