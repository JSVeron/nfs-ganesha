#include "QingStor.h"
#include "qs_file.h"

#include <algorithm>
#include <time.h>

using namespace QingStor;

QsFileHandle::QsFileHandle(QsFileSystem* fs, uint32_t fs_inst, QsFileHandle* _parent,
                           const QsFHKey& _fhk, const std::string& _name, uint32_t _flags)
  : fs(fs), bucket(nullptr), parent(_parent), name(std::move(_name)),
    fhk(_fhk) {

  if (parent->isRoot()) {
    fh.fh_type = QS_FS_TYPE_DIRECTORY;
    //variant_type = directory();
    flags |= QS_FS_FLAG_BUCKET;
  } else {
    bucket = parent->isBucket() ? parent : parent->bucket;
    if (flags & QS_FS_FLAG_DIRECTORY) {
      fh.fh_type = QS_FS_TYPE_DIRECTORY;
      //variant_type = directory();
    } else {
      fh.fh_type = QS_FS_TYPE_FILE;
      //variant_type = file();
    }
  }

  depth = parent->depth + 1;

  /* save constant fhk */
  fh.fh_hk = fhk.fh_hk; /* XXX redundant in fh_hk */

  /* stat */
  state.dev = fs_inst;

  switch (fh.fh_type) {
  case QS_FS_TYPE_DIRECTORY:
    state.unix_mode = QS_RWXMODE | S_IFDIR;
    break;
  case QS_FS_TYPE_FILE:
    state.unix_mode = QS_RWMODE | S_IFREG;
  default:
    break;
  }

  /* pointer to self */
  fh.fh_private = this;
}

const std::string QsFileHandle::getObjectKey(bool omit_bucket) const
{
  std::string path;
  std::vector<const std::string*> segments;
  //int reserve = 0;
  const QsFileHandle* tfh = this;
  while (tfh && !tfh->isRoot() && !(tfh->isBucket() && omit_bucket)) {
    segments.push_back(&tfh->getObjectName());
    //reserve += (1 + tfh->getObjectName().length());
    tfh = tfh->parent;
  }

  bool first = true;
  //path.reserve(reserve);
  std::reverse (segments.begin( ), segments.end( ) );
  for (auto& s : segments) {
    if (!first)
      path += "/";
    else {
      if (!omit_bucket && (path.front() != '/')) // pretty-print
        path += "/";
      first = false;
    }
    path += *s;
  }
  return path;
}


QsFsError QsFileHandle::readDir(qingstor_readdir_callback readCallback, void *cb_arg, uint64_t *offset,
                                bool *eof)
{
  
  Bucket * qsBucket = fs->libSDK.qsBucket;
  ListObjectsInput input;
  ListObjectsOutput output;

  //input.SetPrefix();
  input.SetDelimiter("/");

  ObjectKey marker = findMarker(*offset);
  if ("" != marker) {
    input.SetMarker("/");
  }

  if (!qsBucket)
  {
    // log here
    QsFsError::QS_FS_ERR_UNKNOWN;
  }

  QsError err = qsBucket->listObjects(input, output);
  if ( QsError::QS_ERR_NO_ERROR != err)
  {
    // log here
    return QsFsError::QS_FS_ERR_NTWORK_ERROR;
  }

  auto respCode = output.GetResponseCode();
  if ( Http::HttpResponseCode::OK != respCode) {
    // log here
    return QsFsError::QS_FS_ERR_UNKNOWN;
  }

  // update access time
  timespec time;
  clock_gettime(CLOCK_REALTIME, &time);
  setAtimes(time);

  std::vector<KeyType> keys = output.GetKeys();
  for (auto objkey : output.GetKeys())
  {

    if (!readCallback(objkey.GetKey().c_str(), cb_arg, *offset)) {
      break;
    }
  }

  return QsFsError::QS_FS_ERR_NO_ERROR;;
} /* QsFileHandle::readDir */


void QsFileHandle::setStat(struct stat* st) {
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
  case QS_FS_TYPE_DIRECTORY:
    st->st_nlink = state.nlink;
    break;
  case QS_FS_TYPE_FILE:
    st->st_nlink = 1;
    st->st_blksize = 4096;
    st->st_size = state.size;
    st->st_blocks = (state.size) / 512;
    break;
  default:
    break;
  }
  return;
}

void QsFileHandle::createStat(struct stat* st, uint32_t mask)
{
  if (mask & QS_SETATTR_UID)
    state.owner_uid = st->st_uid;

  if (mask & QS_SETATTR_GID)
    state.owner_gid = st->st_gid;

  if (mask & QS_SETATTR_MODE) {
    switch (fh.fh_type) {
    case QS_FS_TYPE_DIRECTORY:
      state.unix_mode = st->st_mode | S_IFDIR;
      break;
    case QS_FS_TYPE_FILE:
      state.unix_mode = st->st_mode | S_IFREG;
      break;
    default:
      break;
    }
  }

  if (mask & QS_SETATTR_ATIME)
    state.atime = st->st_atim;
  if (mask & QS_SETATTR_MTIME)
    state.mtime = st->st_mtim;
  if (mask & QS_SETATTR_CTIME)
    state.ctime = st->st_ctim;

  return;
}



///////////////
QSFHResult QsFileSystem::lookupFileHandle(QsFileHandle* parent, const std::string name, uint32_t flags) {

  QsFileHandle * fh = nullptr;

  std::string objKey = parent->getObjectKey() + name;
  if (validObjectName(objKey.c_str()))
  {
    // log here
    return QSFHResult(QsFsError::QS_FS_ERR_PATH_TOO_LONG);
  }

  /*
  if(flags & QsFileHandle::QS_FS_FLAG_NONE){
      // just create file handle obj for cache,
      //  no need to nofity storge backend QingStor
      fh = createFileHandle(parent, name, flags);
      fh->setMtimes(output.GetLastModified());
      fh->setSize(output.GetContentLength());

      return QSFHResult(fh);
  }
  */

  HeadObjectInput input;
  HeadObjectOutput output;

  QsError err = libSDK.qsBucket->headObject(objKey, input, output);
  if ( QsError::QS_ERR_NO_ERROR != err)
  {
    // log here
    return QSFHResult(QsFsError::QS_FS_ERR_NTWORK_ERROR);
  }

  auto respCode = output.GetResponseCode();
  if ( Http::HttpResponseCode::NOT_FOUND == respCode) {
    // log here
    return QSFHResult(QsFsError::QS_FS_ERR_FILE_NO_EXIST);
  }
  else if ( Http::HttpResponseCode::OK == respCode) {
    fh = createFileHandle(parent, name, flags);
    fh->setMtimes(output.GetLastModified());
    fh->setSize(output.GetContentLength());

    return QSFHResult(fh);
  }
  else {
    return QSFHResult(QsFsError::QS_FS_ERR_UNKNOWN);
  }
}

QSFHResult QsFileSystem::touchFile(QsFileHandle* parent, const char *name,
                                   struct stat *st, uint32_t mask)
{
  if (!parent->isDir())
  {
    return QSFHResult(QsFsError::QS_FS_ERR_INVAILD_PARENT_PATH);
  }

  return createFileObj(parent, name, st, mask, QsFileHandle::QS_FS_FLAG_NONE);
}

QSFHResult QsFileSystem::mkDir(QsFileHandle* parent, const char *name,
                               struct stat *st, uint32_t mask)
{
  if (!parent->isDir())
  {
    return QSFHResult(QsFsError::QS_FS_ERR_INVAILD_PARENT_PATH);
  }

  return createFileObj(parent, name, st, mask, QsFileHandle::QS_FS_FLAG_DIRECTORY | QsFileHandle::QS_FS_FLAG_NONE);
}

QSFHResult QsFileSystem::createFileObj(QsFileHandle* parent, const std::string name,
                                       struct stat *st, uint32_t mask, uint32_t flags)
{
  // qingstor_file_handle *lfh;
  // with QS_FS_FLAG_NONE means do not notify backend storage(QingStor) to create obj.
  QSFHResult result = lookupFileHandle(parent, name, flags);

  QsFsError err = result.getErr();
  if (QsFsError::QS_FS_ERR_NO_ERROR == err) {
    // log here
    /* conflict! */
    releaseFileHandle(result.getFH());
    return QSFHResult(QS_FS_ERR_FILE_ALEADY_EXIST);
  }
  else if (QsFsError::QS_FS_ERR_NTWORK_ERROR == err) {
    // log here
    return QSFHResult(QS_FS_ERR_NTWORK_ERROR);
  }

  if (QsFsError::QS_FS_ERR_FILE_NO_EXIST == err) {
    // we can create the dir now
    PutObjectInput input;
    PutObjectOutput output;

    input.SetContentType("appliction/x-directory");
    input.SetContentLength(0);

    std::string objectKey = parent->getObjectKey() + name;

    QsError err = libSDK.qsBucket->putObject(objectKey, input, output);
    if ( QsError::QS_ERR_NO_ERROR != err)
    {
      // log here
      return QSFHResult(QS_FS_ERR_NTWORK_ERROR);
    }

    auto respCode = output.GetResponseCode();
    if ( Http::HttpResponseCode::OK == respCode) {
      // log here
      QsFileHandle* fh = createFileHandle(parent, name, flags);
      fh->createStat(st, mask);

      // update access time
      timespec time;
      clock_gettime(CLOCK_REALTIME, &time);
      fh->setTimes(time);
      parent->setMtimes(time);
      parent->setCtimes(time);

      //if (st)
      //fh->stat(st);
      return QSFHResult(fh);
    }
    else {
      // log here
      return QSFHResult(QsFsError::QS_FS_ERR_UNKNOWN);
    }
  }
} /* QsFileSystem::createFileObj */



// utils
bool QsFileSystem::validObjectName(const char* name) {
  if (!name || strlen(name) > 1024)
    return false;
  return true;;
}

int QsFileSystem::rename(QsFileHandle* srcParent, QsFileHandle* dstParent,
                         std::string srcName, std::string dstName)
{
  /* XXX initial implementation: try-copy, and delete if copy
   * succeeds */
  int rc = -EINVAL;

  //real_time t;


  /* atomicity FLAG_LOCK*/
  QSFHResult result = lookupFileHandle(srcParent, srcName, QsFileHandle::QS_FS_FLAG_NONE);
  QsFileHandle* srcFH = result.getFH();
  /* should not happen */
  if (! srcFH) {
    // log here
    //ldout(get_context(), 0) << __func__
    //<< " BUG no such src renaming path="
    //<< src_name
    //<< dendl;
    return -EINVAL;
  }

  /* forbid renaming of directories (unreasonable at scale) */
  if (srcFH->isDir()) {
    /* Log here
    ldout(get_context(), 12) << __func__
    << " rejecting attempt to rename directory path="
    << rgw_fh->full_object_name()
    << dendl;
    */
    return -EPERM;
  }

  /* forbid renaming open files (violates intent, for now) */
  if (srcFH->isOpen()) {
    /* Log here
      ldout(get_context(), 12) << __func__
      << " rejecting attempt to rename open file path="
      << rgw_fh->full_object_name()
      << dendl;
       */
    return -EPERM;
  }

  // t = real_clock::now();

  // do copy op
  std::string objectkey = dstParent->getObjectKey() + dstName;
  std::string moveSource = srcFH->getObjectKey() + srcName;


  PutObjectInput copyInput;
  PutObjectOutput copyOutput;
  copyInput.SetXQSMoveSource(moveSource);
  copyInput.SetContentLength(0);

  QsError err = libSDK.qsBucket->putObject(objectkey, copyInput, copyOutput);
  if (QsError::QS_ERR_NO_ERROR != err)
  {
    // log here :network err
    return -EPERM;
  }

  auto respCode = copyOutput.GetResponseCode();
  if ( Http::HttpResponseCode::OK != respCode) {
    // log here
    return QsFsError::QS_FS_ERR_UNKNOWN;
  }

  // then we can delete the src obj ??
  return QsFsError::QS_FS_ERR_NO_ERROR;
} /* QsFileSystem::rename */




//QsFileSystem::QsFileSystem(LibSDK _libSDK, const char *_uid, const char *_bucket_name, const char* _zone)
QsFileSystem::QsFileSystem(LibSDK _libSDK, const char *_uid)
  : rootFH(this, new_inst()), libSDK(_libSDK), uid(_uid) { //, invalidate_cb(nullptr)
  //invalidate_arg(nullptr), shutdown(false), refcnt(1),
  //uid(_uid), key(_user_id, _key) {

  /* no bucket may be named qs_fs-(.*) */
  char * inst = NULL;
  sprintf(inst, "%d", get_inst());
  std::string fsid = root_name + "qs_fs-" + inst;

  rootFH.initRootFS(fsid /* bucket */, root_name);

  /* pointer to self */
  fs.fs_private = this;

  /* expose public root fh */
  fs.root_fh = rootFH.getWrapperHandle();
}


QsFileHandle* QsFileSystem::createFileHandle(QsFileHandle* parent, const std::string& obj_name, uint32_t flags) {

  QsFHKey fhk = parent->makeFHKey(obj_name);

  return new QsFileHandle(this, get_inst(), parent, fhk, obj_name, flags);
}

void QsFileSystem:: close(){
  //state.flags |= FLAG_CLOSE;
};
//