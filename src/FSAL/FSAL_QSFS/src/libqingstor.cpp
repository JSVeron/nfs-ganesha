#include "libqingstor.h"
#include "qs_file.h"

/*
 * release file handle
 */

void qingstor_fh_release(struct qingstor_file_system *qs_fs, struct qingstor_file_handle *qs_fh,
                         uint32_t flags)
{
  QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);
  QsFileHandle *fh = static_cast<QsFileHandle*>(qs_fh->fh_private);//getQsFileHandle(qs_fh);

  // Log here

  fs->releaseFileHandle(fh);
  return;
}

/*
  lookup object by name (POSIX style)
*/
int qingstor_lookup(struct qingstor_file_system *qs_fs,
                    struct qingstor_file_handle *parent_fh, const char* path,
                    struct qingstor_file_handle **out_fh,
                    uint32_t flags)
{
  //CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);
  QsFileHandle* parent = static_cast<QsFileHandle*>(parent_fh->fh_private);
  if ((! parent) || (! parent->isDir())) {
    /* bad parent */
    return -EINVAL;
  }

  QsFileHandle* fh;
  QSFHResult reuslt(QS_FS_ERR_NO_ERROR);

  if (parent->isRoot() &&
      unlikely((strcmp(path, "..") == 0) || (strcmp(path, "/") == 0))) {
    fh = parent;
  }
  else {
    reuslt = fs->lookupFileHandle(parent, path, flags);
    if (QsFsError::QS_FS_ERR_NO_ERROR == reuslt.getErr())
      fh = reuslt.getFH();
    else
      return -ENOENT;
  }

  //struct qingstor_file_handle *rfh = fh->get_fh();
  if (fh) {
    *out_fh = fh->getWrapperHandle();
  }

  return 0;
} /* qingstor_lookup */

/*
  lookup object by handle (NFS style)
*/
int qingstor_lookup_handle(struct qingstor_file_system *qs_fs, struct qingstor_fh_hk *fh_hk,
                           struct qingstor_file_handle **fh, uint32_t flags)
{
  QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);

  //QsFileHandle* qs_fh = fs->lookup_handle(*fh_hk);
 // if (! qs_fh) {
    /* not found */
    //return -ENOENT;
 // }

  //struct qingstor_file_handle *rfh = qs_fh->get_fh();
  //*fh = rfh;

  return 0;
}

/*
  lookup object by name (POSIX style)
*/
int qingstor_touchfile(struct qingstor_file_system *qs_fs,
                       struct qingstor_file_handle *parent_fh,
                       const char* path, struct stat *st, uint32_t mask,
                       struct qingstor_file_handle **out_fh)
{
  //CephContext* cct = static_cast<CephContext*>(rgw_fs->rgw);
  QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);
  QsFileHandle* parent = static_cast<QsFileHandle*>(parent_fh->fh_private);
  if ((! parent) || (! parent->isDir())) {
    /* bad parent */
    return -EINVAL;
  }

  QsFileHandle* fh;


  if (parent->isRoot() &&
      unlikely((strcmp(path, "..") == 0) || (strcmp(path, "/") == 0))) {
    fh = parent;
  }
  else {
    QSFHResult reuslt = fs->touchFile(parent, path, st, mask);
    if (QsFsError::QS_FS_ERR_NO_ERROR == reuslt.getErr())
      fh = reuslt.getFH();
    else
      return -ENOENT;
  }

  //struct qingstor_file_handle *rfh = fh->get_fh();
  if (fh) {
    *out_fh = fh->getWrapperHandle();
  }

  return 0;
} /* qingstor_touchfile */


int qingstor_readdir(struct qingstor_file_system *qs_fs,
                     struct qingstor_file_handle *dir_fh, uint64_t *offset,
                     qingstor_readdir_callback rcb, void *cb_arg, bool *eof)
{
  QsFileHandle* dirFH = static_cast<QsFileHandle*>(dir_fh->fh_private);
  if (! dirFH) {
    /* bad parent */
    return -EINVAL;
  }
  int rc = dirFH->readDir(rcb, cb_arg, offset, eof);
  return rc;
}

/*
   get unix attributes for object
*/
void qingstor_getattr(struct qingstor_file_handle *qs_fh, struct stat *st)
{
  //QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);
  QsFileHandle* fh = static_cast<QsFileHandle*>(qs_fh->fh_private);

  fh->setStat(st);

  return;
}

/*
  set unix attributes for object
*/
int qingstor_setattr(struct qingstor_file_handle *qs_fh, struct stat *st,
                     uint32_t mask)
{
  QsFileHandle* fh = static_cast<QsFileHandle*>(qs_fh->fh_private);

  fh->createStat(st, mask);

  timespec time;
  clock_gettime(CLOCK_REALTIME, &time);
  fh->setCtimes(time);

  return 0;
}

/*
  rename object
*/
int qingstor_rename(struct qingstor_file_system *qs_fs,
                    struct qingstor_file_handle *src, const char* src_name,
                    struct qingstor_file_handle *dst, const char* dst_name)
{
  QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);

  QsFileHandle* src_fh = static_cast<QsFileHandle*>(src->fh_private);
  QsFileHandle* dst_fh = static_cast<QsFileHandle*>(dst->fh_private);

  return fs->rename(src_fh, dst_fh, src_name, dst_name);
}


/*
 attach QingStor namespace
*/
int qingstor_mount( libqs_t libqsfs, const char *uid, const char *bucket_name,
                   const char *zone, struct qingstor_file_system **qs_fs,
                   uint32_t flags)
{
  int rc = 0;
  if ( bucket_name || zone )
  {
    // log here
    // invaild bucket_name or zone, faild to mount
    rc = -EINVAL;
  }

  LibSDK *libSDK = (LibSDK *) libqsfs;
  QingStor::QsConfig qsConfig;
  if (libSDK->qsService)
  {
    qsConfig = libSDK->qsService->GetConfig();
  }

  libSDK->qsBucket = new Bucket(qsConfig, bucket_name, zone);

  /* stash access data for "mount" */
  QsFileSystem* new_fs = new QsFileSystem(*libSDK, uid);
  //assert(new_fs);

  // authorize?????
  /*
  rc = new_fs->authorize(rgwlib.get_store());
  if (rc != 0) {
    delete new_fs;
    return -EINVAL;
  }
  */

  struct qingstor_file_system *fs = new_fs->getFS();
  fs->libqsfs = libqsfs;

  /* XXX we no longer assume "/" is unique, but we aren't tracking the
   * roots atm */

  *qs_fs = fs;

  return 0;
}

/*
 detach QingStor namespace
*/
int qingstor_umount(qingstor_file_system * qs_fs, uint32_t flags)
{
  QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);
  if (fs)
  {
    fs->close();
  }

  return 0;
}



//////////////////////////////////////////
/*
int rgw_readdir(struct rgw_fs *rgw_fs,
    struct rgw_file_handle *parent_fh, uint64_t *offset,
    rgw_readdir_cb rcb, void *cb_arg, bool *eof,
    uint32_t flags)
{
  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if (! parent) {
    /// bad parent
    return -EINVAL;
  }
  int rc = parent->readdir(rcb, cb_arg, offset, eof, flags);

   // 调用C SDK接口发送create请求
  qs_list_buckets_input_t input;
  qs_list_buckets_output_t output;
  input.content_length = 0;

  QsError err = qs_list_buckets(name, &input, &output);
  if (err == QS_ERROR_NO_ERROR){
    // fill in stat data
    timespec current_time;
    clock_gettime(CLOCK_REALTIME, &current_time);

    parent.state.mtime = current_time;
    parent.state.ctime = current_time;

  }
  else{

    // log
  }


  return rc;
}







// 内部维护的文件state结构，通过state方法获取时，方法内部根据自定义state结构内容，填充标准stat结构内容
    struct State {
      uint64_t dev;
      uint64_t size;
      uint64_t nlink;
      uint32_t owner_uid; // XXX need Unix attr
      uint32_t owner_gid; // XXX need Unix attr
      mode_t unix_mode;
      struct timespec ctime;
      struct timespec mtime;
      struct timespec atime;
      State() : dev(0), size(0), nlink(1), owner_uid(0), owner_gid(0),
    ctime{0,0}, mtime{0,0}, atime{0,0} {}
    } state;


    int stat(struct stat* st) {
      // partial Unix attrs
      memset(st, 0, sizeof(struct stat));
      st->st_dev = state.dev;
      st->st_ino = fh.fh_hk.object; // XXX

      st->st_uid = state.owner_uid;
      st->st_gid = state.owner_gid;

      st->st_mode = state.unix_mode;

#ifdef HAVE_STAT_ST_MTIMESPEC_TV_NSEC
      st->st_atimespec = state.atime;
      st->st_mtimespec = state.mtime;
      st->st_ctimespec = state.ctime;
#else
      st->st_atim = state.atime;
      st->st_mtim = state.mtime;
      st->st_ctim = state.ctime;
#endif

      switch (fh.fh_type) {
      case RGW_FS_TYPE_DIRECTORY:
  st->st_nlink = state.nlink;
  break;
      case RGW_FS_TYPE_FILE:
  st->st_nlink = 1;
  st->st_blksize = 4096;
  st->st_size = state.size;
  st->st_blocks = (state.size) / 512;
      default:
  break;
      }

      return 0;
    }


/*
  generic create -- create an empty regular file
*/
/*
int qingstor_create(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
         const char *name, struct stat *st, uint32_t mask,
         struct rgw_file_handle **fh, uint32_t posix_flags,
         uint32_t flags)
{
  using std::get;

  RGWLibFS *fs = static_cast<RGWLibFS*>(rgw_fs->fs_private);
  RGWFileHandle* parent = get_rgwfh(parent_fh);

  if ((! parent) ||
      (parent->is_root()) ||
      (parent->is_file())) {
    // bad parent
    return -EINVAL;
  }


  // 1.检查是否已存在相同的名字的object
    rc = rgw_lookup(get_fs(), parent->get_fh(), name, &lfh,
        RGW_LOOKUP_FLAG_NONE);
    if (! rc) {
      // conflict!
      rc = rgw_fh_rele(get_fs(), lfh, RGW_FH_RELE_FLAG_NONE);
      return MkObjResult{nullptr, -EEXIST};
    }
  // 2.检查名字是否合法
    // expand and check name
    if(!valid_fs_object_name(name)){
      return //;
    }

    // 调用C SDK接口发送create请求
  qs_put_object_input_t input;
  qs_put_object_output_t output;
  input.content_length = 0;

    QsError err = qs_put_object(name, &input, &output);
    if (err == QS_ERROR_NO_ERROR){
    // fill in stat data
    timespec current_time;
      clock_gettime(CLOCK_REALTIME, &current_time);

    parent.state.mtime = current_time;
    parent.state.ctime = current_time;

    }
    else{

      // log
    }


  return get<1>(fhr);
}
*/
/* qingstor_create*/



//////////////////////////////////////////////////////////////////////////////////
//libqsfs
int librqs_create(libqs_t* libqsfs, const char* conf_path)
{
  using namespace QingStor;

  int rc = -EINVAL;

  QingStorService::initService("./");
  QingStor::QsConfig qsConfig;
  qsConfig.loadConfigFile(conf_path);

  LibSDK* libSDK = new LibSDK();
  libSDK->qsService = new QingStorService(qsConfig);
  //Bucket* qsBucket = new Bucket(qsConfig, "huang-stor", "pek3a");;
  *libqsfs = libSDK;

  return rc;
}

void librgw_shutdown(libqs_t libqsfs)
{
  using namespace QingStor;

  // add time to confirm safe thread op.
  QingStorService::shutdownService();
  //delete libqsfs->
}
