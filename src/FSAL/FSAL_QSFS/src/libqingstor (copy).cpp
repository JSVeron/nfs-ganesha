
/*
 * release file handle
 */
void qingstor_fh_release(struct qingstor_file_system *qs_fs, struct qingstor_file_handle *qs_fh,
    uint32_t flags)
{
  QsFileSystem *fs = static_cast<QSFileSystem*>(qs_fs->fs_private);
  QsFileHandle *fh = static_cast<RGWFileHandle*>(qs_fh->fh_private);//getQsFileHandle(qs_fh);

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
    if ((! parent) || (! parent->is_dir())) {
      /* bad parent */
      return -EINVAL;
    }

    QsFileHandle* fh;
    QSFHResult reuslt;

     if (parent->is_root() && 
        unlikely((strcmp(path, "..") == 0) || (strcmp(path, "/") == 0))) {
            fh = parent;
        }
     }
     else{
          reuslt = fs->lookupFileHandle(parent, path, flags);
          if(QsFsError::QS_FS_ERR_NO_ERROR == reuslt.err)
              fh = reuslt.fh; 
          else
              return -ENOENT;     
     }

    //struct qingstor_file_handle *rfh = fh->get_fh();
     if(fh){
        *out_fh = fh->getWrapperHandle();
     }

    return 0;
} /* rgw_lookup */


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
    if ((! parent) || (! parent->is_dir())) {
      /* bad parent */
      return -EINVAL;
    }

    QsFileHandle* fh;
    QSFHResult reuslt;

     if (parent->is_root() && 
        unlikely((strcmp(path, "..") == 0) || (strcmp(path, "/") == 0))) {
            fh = parent;
        }
     }
     else{
          reuslt = fs->touchFile(parent, path, st, mask);
          if(QsFsError::QS_FS_ERR_NO_ERROR == reuslt.err)
              fh = reuslt.fh; 
          else
              return -ENOENT; 
     }

    //struct qingstor_file_handle *rfh = fh->get_fh();
     if(fh){
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
  int rc = dirFH->readDir(rcb, cb_arg, offset, eof, flags);
  return rc;
}

/*
   get unix attributes for object
*/
int qingstor_getattr(struct qingstor_file_handle *qs_fh, struct stat *st)
{
  //QsFileSystem *fs = static_cast<QsFileSystem*>(qs_fs->fs_private);
  QsFileHandle* fh = static_cast<QsFileHandle*>(qs_fh->fh_private);

  return fh->getStat(st);
}

/*
  set unix attributes for object
*/
int qingstor_setattr(struct qingstor_file_handle *qs_fh, struct stat *st,
    uint32_t mask)
{
    QsFileHandle* fh = static_cast<QsFileHandle*>(qs_fh->fh_private);

    fh->createStat(st, mask);
    fh->setCtime(real_clock::to_timespec(real_clock::now()));

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


//////////////////////////////////////////










  /**
 * @brief Callback to provide readdir caller with each directory entry
 *
 * The called function will indicate if readdir should continue, terminate,
 * terminate and mark cookie, or continue and mark cookie. In the last case,
 * the called function may also return a cookie if requested in the ret_cookie
 * parameter (which may be NULL if the caller doesn't need to mark cookies).
 * If ret_cookie is 0, the caller had no cookie to return.
 *
 * @param[in]      name         The name of the entry
 * @param[in]      obj          The fsal_obj_handle describing the entry
 * @param[in]      attrs        The requested attribues for the entry (see
 *                              readdir attrmask parameter)
 * @param[in]      dir_state    Opaque pointer to be passed to callback
 * @param[in]      cookie       An FSAL generated cookie for the entry
 *
 * @returns fsal_dir_result above
 */
typedef enum fsal_dir_result (*fsal_readdir_cb)(
        const char *name, struct fsal_obj_handle *obj,
        struct attrlist *attrs,
        void *dir_state, fsal_cookie_t cookie);
/**





int rgw_readdir(struct rgw_fs *rgw_fs,
    struct rgw_file_handle *parent_fh, uint64_t *offset,
    rgw_readdir_cb rcb, void *cb_arg, bool *eof,
    uint32_t flags)
{
  RGWFileHandle* parent = get_rgwfh(parent_fh);
  if (! parent) {
    /* bad parent */
    return -EINVAL;
  }
  int rc = parent->readdir(rcb, cb_arg, offset, eof, flags);

   // 调用C SDK接口发送create请求
  qs_list_buckets_input_t input;
  qs_list_buckets_output_t output;
  input.content_length = 0;

  QsError err = qs_list_buckets(name, &input, &output);
  if (err == QS_ERROR_NO_ERROR){
    /* fill in stat data */
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
      uint32_t owner_uid; /* XXX need Unix attr */
      uint32_t owner_gid; /* XXX need Unix attr */
      mode_t unix_mode;
      struct timespec ctime;
      struct timespec mtime;
      struct timespec atime;
      State() : dev(0), size(0), nlink(1), owner_uid(0), owner_gid(0),
    ctime{0,0}, mtime{0,0}, atime{0,0} {}
    } state;


    int stat(struct stat* st) {
      /* partial Unix attrs */
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
    /* bad parent */
    return -EINVAL;
  }

  
  // 1.检查是否已存在相同的名字的object
    rc = rgw_lookup(get_fs(), parent->get_fh(), name, &lfh,
        RGW_LOOKUP_FLAG_NONE);
    if (! rc) {
      /* conflict! */
      rc = rgw_fh_rele(get_fs(), lfh, RGW_FH_RELE_FLAG_NONE);
      return MkObjResult{nullptr, -EEXIST};
    }
  // 2.检查名字是否合法
    /* expand and check name */
    if(!valid_fs_object_name(name)){
      return //;
    }

    // 调用C SDK接口发送create请求
  qs_put_object_input_t input;
  qs_put_object_output_t output;
  input.content_length = 0;

    QsError err = qs_put_object(name, &input, &output);
    if (err == QS_ERROR_NO_ERROR){
    /* fill in stat data */
    timespec current_time;
      clock_gettime(CLOCK_REALTIME, &current_time); 

    parent.state.mtime = current_time;
    parent.state.ctime = current_time;

    }
    else{
  
      // log 
    }


  return get<1>(fhr);
} /* rgw_create */


/*
 attach qingstor namespace
*/
  int qingstor_mount(libqs_t libqs, const char *uid, const char *acc_key,
		const char *sec_key, struct rgw_fs **rgw_fs,
		uint32_t flags)
{
  int rc = 0;

  /* stash access data for "mount" */
  RGWLibFS* new_fs = new RGWLibFS(static_cast<CephContext*>(rgw), uid, acc_key,
				  sec_key);
  assert(new_fs);

  rc = new_fs->authorize(rgwlib.get_store());
  if (rc != 0) {
    delete new_fs;
    return -EINVAL;
  }

  /* register fs for shared gc */
  rgwlib.get_fe()->get_process()->register_fs(new_fs);

  struct rgw_fs *fs = new_fs->get_fs();
  fs->rgw = rgw;

  /* XXX we no longer assume "/" is unique, but we aren't tracking the
   * roots atm */

  *rgw_fs = fs;

  return 0;
}