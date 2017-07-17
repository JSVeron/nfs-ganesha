#define QS_LOOKUP_FLAG_NONE    0x0000
#define QS_LOOKUP_FLAG_CREATE_BACKEND  0x0001


/*
 * release file handle
 */
int qingstor_fh_release(struct qingstor_file_system *qs_fs, struct qingstor_file_handle *qs_fh,
    uint32_t flags);

/*
 * lookup file/dir handle
 */
int qingstor_lookup(struct qingstor_file_system *qs_fs,
        struct qingstor_file_handle *parent_fh, const char* path,
        struct qingstor_file_handle **out_fh
        uint32_t flags)

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
    qingstor_readdir_callback rcb, void *cb_arg, bool *eof)

//////////////////////
/*
   get unix attributes for object
*/
#define RGW_GETATTR_FLAG_NONE      0x0000

int qingstor_getattr(struct rgw_fs *rgw_fs,
    struct rgw_file_handle *fh, struct stat *st,
    uint32_t flags);

/*
   set unix attributes for object
*/
#define RGW_SETATTR_FLAG_NONE      0x0000

int qingstor_setattr(struct rgw_fs *rgw_fs,
    struct rgw_file_handle *fh, struct stat *st,
    uint32_t mask, uint32_t flags);


typedef bool (*rgw_readdir_cb)(const char *name, void *arg, uint64_t offset,
             uint32_t flags);

////////////////////////////////////////////////////////////////////////////////////////////////
typedef void* libqs_t;

int libqs_create(librqs_t *rgw, int argc, char **argv); //库初始化
void libqs_shutdown(librqs_t rgw); //库去初始化


/*
 * object types
 */
enum qingstor_fh_type {
  QINGSTOR_FS_TYPE_NIL = 0,
  QINGSTOR_FS_TYPE_FILE,
  QINGSTOR_TYPE_DIRECTORY,
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
  void *fh_private; /* libqs private data */ 
  /* object type */
  enum qingstor_fh_type fh_type; // 对象类型
};

struct qingstor_file_system
{
  libqs_t qingstor;
  void *fs_private;
  struct qingstor_file_handle* root_fh; // 根文件
};

////////////////////////////////////////////////////////////////////////////////////////////////
/*
 attach rgw namespace
*/
#define RGW_MOUNT_FLAG_NONE     0x0000

int rgw_mount(librgw_t rgw, const char *uid, const char *key,
	      const char *secret, struct rgw_fs **rgw_fs,
	      uint32_t flags);


struct State {
  std::mutex mtx;
  std::atomic<uint32_t> flags;
  std::deque<event> events;

  State() : flags(0) {}

  void push_event(const event& ev) {
events.push_back(ev);
  }
} state;


/////////////////////////////////////////////
