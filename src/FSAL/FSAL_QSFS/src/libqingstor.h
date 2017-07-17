#pragma once
#include <inttypes.h>

#define likely(x)   __builtin_expect((x),1)
#define unlikely(x)   __builtin_expect((x),0)

#define QS_LOOKUP_FLAG_NONE    0x0000
#define QS_LOOKUP_FLAG_CREATE_BACKEND  0x0001

#define QS_SETATTR_MODE    1
#define QS_SETATTR_UID     2
#define QS_SETATTR_GID     4
#define QS_SETATTR_MTIME   8
#define QS_SETATTR_ATIME  16
#define QS_SETATTR_SIZE   32
#define QS_SETATTR_CTIME  64

////////////////////////////////////////////////////////////////////////////////////////////////
typedef void* libqs_t;

int libqsfs_create(libqs_t *libqsfs, int argc, char **argv); //库初始化
void libqsfs_shutdown(libqs_t libqsfs); //库去初始化

///////////////////////////////////////////////////////////////////////////////////////////////

/*
 * release file handle
 */
void qingstor_fh_release(struct qingstor_file_system *qs_fs, struct qingstor_file_handle *qs_fh,
                         uint32_t flags);

/*
 * lookup file/dir handle
 */
int qingstor_lookup(struct qingstor_file_system *qs_fs,
                    struct qingstor_file_handle *parent_fh, const char* path,
                    struct qingstor_file_handle **out_fh,
                    uint32_t flags);

/*
 * create file/dir handle
 */
int qingstor_touchfile(struct qingstor_file_system *qs_fs,
                       struct qingstor_file_handle *parent_fh,
                       const char* path, struct stat *st, uint32_t mask,
                       struct qingstor_file_handle **out_fh);

/*
    read  directory callback
*/
typedef bool (*qingstor_readdir_callback)(const char *name, void *arg, uint64_t offset);
/*

/*
    read  directory content
*/
int qingstor_readdir(struct qingstor_file_system *qs_fs,
                     struct qingstor_file_handle *parent_fh, uint64_t *offset,
                     qingstor_readdir_callback rcb, void *cb_arg, bool *eof);


/*
   get unix attributes for object
*/
#define QS_GETATTR_FLAG_NONE      0x0000

void qingstor_getattr(struct qingstor_file_system *qs_fs,
                      struct qingstor_file_handle *fh, struct stat *st,
                      uint32_t flags);

/*
   set unix attributes for object
*/
#define QS_SETATTR_FLAG_NONE      0x0000

int qingstor_setattr(struct qingstor_file_system *qs_fs,
                     struct qingstor_file_handle *fh, struct stat *st,
                     uint32_t mask, uint32_t flags);



/*
   set unix attributes for object
*/
int qingstor_rename(struct qingstor_file_system *qs_fs,
                    struct qingstor_file_handle *src, const char* src_name,
                    struct qingstor_file_handle *dst, const char* dst_name);

/*
 attach QingStor namespace
*/
#define QS_MOUNT_FLAG_NONE     0x0000

int qingstor_mount(libqs_t libqsfs, const char *uid, const char *bucket_name,
                   const char *zone, struct qingstor_file_system *qs_fs,
                   uint32_t flags);




/*
 * object types
 */
enum qingstor_fh_type {
  QS_FS_TYPE_NIL = 0,
  QS_FS_TYPE_FILE,
  QS_FS_TYPE_DIRECTORY,
};
/* content-addressable hash */
struct qingstor_fh_hk {
  uint64_t bucket;
  uint64_t object;
};


struct qingstor_file_handle
{
  /* content-addressable hash */
  struct qingstor_fh_hk fh_hk; // （对象）文件的hashkey
  void *fh_private; /* libqsfs private data */
  /* object type */
  qingstor_fh_type fh_type; // 对象类型
};

struct qingstor_file_system
{
  libqs_t libqsfs;
  void *fs_private;
  struct qingstor_file_handle* root_fh; // 根文件
};

/* XXX mount info hypothetical--emulate Unix, support at least
 * UUID-length fsid */
struct qingstor_statvfs {
  uint64_t  f_bsize;    /* file system block size */
  uint64_t  f_frsize;   /* fragment size */
  uint64_t     f_blocks;   /* size of fs in f_frsize units */
  uint64_t     f_bfree;    /* # free blocks */
  uint64_t     f_bavail;   /* # free blocks for unprivileged users */
  uint64_t     f_files;    /* # inodes */
  uint64_t     f_ffree;    /* # free inodes */
  uint64_t     f_favail;   /* # free inodes for unprivileged users */
  uint64_t     f_fsid[2];     /* file system ID */
  uint64_t     f_flag;     /* mount flags */
  uint64_t     f_namemax;  /* maximum filename length */
};

////////////////////////////////////////////////////////////////////////////////////////////////
/*

struct State {
  std::mutex mtx;
  std::atomic<uint32_t> flags;
  std::deque<event> events;

  State() : flags(0) {}

  void push_event(const event& ev) {
events.push_back(ev);
  }
} state;

*/
/////////////////////////////////////////////
