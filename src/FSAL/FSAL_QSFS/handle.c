/*
// +-------------------------------------------------------------------------
// | Copyright (C) 2017 Yunify, Inc.
// +-------------------------------------------------------------------------
// | Licensed under the Apache License, Version 2.0 (the "License");
// | you may not use this work except in compliance with the License.
// | You may obtain a copy of the License in the LICENSE file, or at:
// |
// | http://www.apache.org/licenses/LICENSE-2.0
// |
// | Unless required by applicable law or agreed to in writing, software
// | distributed under the License is distributed on an "AS IS" BASIS,
// | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// | See the License for the specific language governing permissions and
// | limitations under the License.
// +-------------------------------------------------------------------------
*/

#include <fcntl.h>
#include "fsal.h"
#include "fsal_types.h"
#include "fsal_convert.h"
#include "fsal_api.h"
#include "internal.h"
#include "nfs_exports.h"
#include "FSAL/fsal_commonlib.h"

extern __thread struct req_op_context *op_ctx;

/**
 * @brief Release an object
 *
 * @param[in] obj_hdl The object to release
 *
 * @return FSAL status codes.
 */

static void release(struct fsal_obj_handle *obj_hdl)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl %p =====================",
	        __func__, obj_hdl);

	struct qs_fsal_handle *obj =
	    container_of(obj_hdl, struct qs_fsal_handle, handle);
	struct qs_fsal_export *export = obj->export;

	// if the file to release is the root handle
	if (obj->qs_fh != export->qs_fs->root_fh) {
		/* release QINGSTOR ref */
		(void) qingstor_fh_release(export->qs_fs, obj->qs_fh, 0 /* flags */);
	}
	deconstruct_handle(obj);
}

/**
 * @brief Look up an object by name
 *
 * This function looks up an object by name in a directory.
 *
 * @param[in]     dir_hdl    The directory in which to look up the object.
 * @param[in]     path       The name to look up.
 * @param[in,out] obj_hdl    The looked up object.
 * @param[in,out] attrs_out  Optional attributes for newly created object
 *
 * @return FSAL status codes.
 */
static fsal_status_t _lookup_private(struct fsal_obj_handle *dir_hdl,
                                     const char *path, struct fsal_obj_handle **obj_hdl,
                                     struct attrlist *attrs_out,
                                     uint32_t flags)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with dir_hdl:%p,  obj_hdl: %p, path: %s =====================",
	        __func__, dir_hdl, obj_hdl, path);
	int rc;
	struct stat st;
	struct qingstor_file_handle *qs_fh;
	struct qs_fsal_handle *obj;

	struct qs_fsal_export *export =
	    container_of(op_ctx->fsal_export, struct qs_fsal_export, export);

	struct qs_fsal_handle *dir = container_of(dir_hdl, struct qs_fsal_handle,
	                             handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter dir_hdl %p path %s", __func__, dir_hdl, path);

	/* XXX presently, we can only fake attrs--maybe rgw_lookup should
	 * take struct stat pointer OUT as libcephfs' does */
	rc = qingstor_lookup(export->qs_fs, dir->qs_fh, path, &qs_fh,
	                     flags);
	if (rc < 0)
		return qs2fsal_error(rc);

	rc = qingstor_getattr(export->qs_fs, qs_fh, &st, QS_GETATTR_FLAG_NONE);
	if (rc < 0)
		return qs2fsal_error(rc);

	rc = construct_handle(export, qs_fh, &st, &obj);
	if (rc < 0) {
		return qs2fsal_error(rc);
	}

	*obj_hdl = &obj->handle;

	if (attrs_out != NULL) {
		posix2fsal_attributes_all(&st, attrs_out);
	}

	return fsalstat(0, 0);
}


/**
 * @brief Look up an object by name
 *
 * This function looks up an object by name in a directory.
 *
 * @param[in]     dir_hdl    The directory in which to look up the object.
 * @param[in]     path       The name to look up.
 * @param[in,out] obj_hdl    The looked up object.
 * @param[in,out] attrs_out  Optional attributes for newly created object
 *
 * @return FSAL status codes.
 */
static fsal_status_t lookup(struct fsal_obj_handle *dir_hdl,
                            const char *path, struct fsal_obj_handle **obj_hdl,
                            struct attrlist *attrs_out)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p, path: %s =====================",
	        __func__, obj_hdl, path);

	return _lookup_private(dir_hdl, path, obj_hdl, attrs_out,
	                       QS_LOOKUP_FLAG_NONE);
}



/**
 * @brief Create a regular file
 *
 * This function creates an empty, regular file.
 *
 * @param[in]     dir_hdl    Directory in which to create the file
 * @param[in]     name       Name of file to create
 * @param[in]     attrs_in   Attributes of newly created file
 * @param[in,out] obj_hdl    Handle for newly created file
 * @param[in,out] attrs_out  Optional attributes for newly created object
 *
 * @return FSAL status.
 */

static fsal_status_t qs_fsal_create(struct fsal_obj_handle *dir_hdl,
                                    const char *name,
                                    struct attrlist *attrs_in,
                                    struct fsal_obj_handle **obj_hdl,
                                    struct attrlist *attrs_out)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with dir_hdl:%p,  obj_hdl: %p, name: %s =====================",
	        __func__, dir_hdl, obj_hdl, name);

	int rc;
	struct qingstor_file_handle *qs_fh;
	struct qs_fsal_handle *obj;
	struct stat st;

	struct qs_fsal_export *export =
	    container_of(op_ctx->fsal_export, struct qs_fsal_export, export);

	struct qs_fsal_handle *dir = container_of(dir_hdl, struct qs_fsal_handle,
	                             handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter dir_hdl %p name %s", __func__, dir_hdl, name);

	memset(&st, 0, sizeof(struct stat));

	st.st_uid = op_ctx->creds->caller_uid;
	st.st_gid = op_ctx->creds->caller_gid;
	st.st_mode = fsal2unix_mode(attrs_in->mode)
	             & ~op_ctx->fsal_export->exp_ops.fs_umask(op_ctx->fsal_export);

	uint32_t create_mask =
	    QS_SETATTR_UID | QS_SETATTR_GID | QS_SETATTR_MODE;

	rc = qingstor_touchfile(export->qs_fs, dir->qs_fh, name, &st,
	                        create_mask, &qs_fh);
	if (rc < 0)
		return qs2fsal_error(rc);

	rc = construct_handle(export, qs_fh, &st, &obj);
	if (rc < 0) {
		return qs2fsal_error(rc);
	}

	*obj_hdl = &obj->handle;

	if (attrs_out != NULL) {
		posix2fsal_attributes_all(&st, attrs_out);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/**
 * @brief Create a directory
 *
 * This function creates a new directory.
 *
 * For support_ex, this method will handle attribute setting. The caller
 * MUST include the mode attribute and SHOULD NOT include the owner or
 * group attributes if they are the same as the op_ctx->cred.
 *
 * @param[in]     dir_hdl Directory in which to create the directory
 * @param[in]     name    Name of directory to create
 * @param[in]     attrib  Attributes to set on newly created object
 * @param[out]    new_obj Newly created object
 *
 * @note On success, @a new_obj has been ref'd
 *
 * @return FSAL status.
 */

static fsal_status_t qs_fsal_mkdir(struct fsal_obj_handle *dir_hdl,
                                   const char *name, struct attrlist *attrs_in,
                                   struct fsal_obj_handle **obj_hdl,
                                   struct attrlist *attrs_out)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with dir_hdl:%p,  obj_hdl: %p, name: %s =====================",
	        __func__, dir_hdl, obj_hdl, name);

	int rc;
	struct qingstor_file_handle *qs_fh;
	struct qs_fsal_handle *obj;
	struct stat st;

	struct qs_fsal_export *export =
	    container_of(op_ctx->fsal_export, struct qs_fsal_export, export);

	struct qs_fsal_handle *dir = container_of(dir_hdl, struct qs_fsal_handle,
	                             handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter dir_hdl %p name %s", __func__, dir_hdl, name);

	memset(&st, 0, sizeof(struct stat));

	st.st_uid = op_ctx->creds->caller_uid;
	st.st_gid = op_ctx->creds->caller_gid;
	st.st_mode = fsal2unix_mode(attrs_in->mode)
	             & ~op_ctx->fsal_export->exp_ops.fs_umask(op_ctx->fsal_export);

	uint32_t create_mask =
	    QS_SETATTR_UID | QS_SETATTR_GID | QS_SETATTR_MODE;

	rc = qingstor_mkdir(export->qs_fs, dir->qs_fh, name, &st,
	                    create_mask, &qs_fh, QS_MKDIR_FLAG_NONE);
	if (rc < 0)
		return qs2fsal_error(rc);

	rc = construct_handle(export, qs_fh, &st, &obj);
	if (rc < 0) {
		return qs2fsal_error(rc);
	}

	*obj_hdl = &obj->handle;

	if (attrs_out != NULL) {
		posix2fsal_attributes_all(&st, attrs_out);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/*
struct qs_fsal_cb_arg {
	fsal_readdir_cb cb;
	void *fsal_arg;
	struct fsal_obj_handle *dir_hdl;
	attrmask_t attrmask;
};
*/

static bool qingstor_rcb(const char *name, void *arg, uint64_t offset)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with name: %s =====================",
	        __func__, name);

	struct attrlist attrs;
	//struct fsal_obj_handle *obj_hdl;
	fsal_status_t status;

	enum fsal_dir_result cb_rc;

	struct fsal_obj_handle *obj;
	struct qs_fsal_cb_arg *qs_cb_arg = (struct qs_fsal_cb_arg *)arg;

	fsal_prepare_attrs(&attrs, qs_cb_arg->attrmask);

	status = _lookup_private(qs_cb_arg->dir_hdl, name, &obj, &attrs , QS_LOOKUP_FLAG_NONE);
	if (FSAL_IS_ERROR(status))
		return false;

	/** @todo FSF - when rgw gains mark capability, need to change this
	 *              code...
	 */
	cb_rc = qs_cb_arg->cb(name, obj, &attrs, qs_cb_arg->fsal_arg, offset);

	fsal_release_attrs(&attrs);

	return cb_rc <= DIR_READAHEAD;
}

/**
 * @brief Read a directory
 *
 * This function reads the contents of a directory (excluding . and
 * .., which is ironic since the Ceph readdir call synthesizes them
 * out of nothing) and passes dirent information to the supplied
 * callback.
 *
 * @param[in]  dir_hdl     The directory to read
 * @param[in]  whence      The cookie indicating resumption, NULL to start
 * @param[in]  dir_state   Opaque, passed to cb
 * @param[in]  cb          Callback that receives directory entries
 * @param[out] eof         True if there are no more entries
 *
 * @return FSAL status.
 */

static fsal_status_t qs_fsal_readdir(struct fsal_obj_handle *dir_hdl,
                                     fsal_cookie_t *whence, void *cb_arg,
                                     fsal_readdir_cb cb, attrmask_t attrmask,
                                     bool *eof)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with dir_hdl:%p =====================",
	        __func__, dir_hdl);

	int rc;
	fsal_status_t fsal_status = {ERR_FSAL_NO_ERROR, 0};
	struct qs_fsal_cb_arg qs_cb_arg = {cb, cb_arg, dir_hdl, attrmask};
	uint64_t r_whence = (whence) ? *whence : 0;

	struct qs_fsal_export *export =
	    container_of(op_ctx->fsal_export, struct qs_fsal_export, export);

	struct qs_fsal_handle *dir = container_of(dir_hdl, struct qs_fsal_handle,
	                             handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter dir_hdl %p", __func__, dir_hdl);

	rc = 0;
	*eof = false;
	rc = qingstor_readdir(export->qs_fs, dir->qs_fh, &r_whence, qingstor_rcb,
	                      &qs_cb_arg, eof);
	if (rc < 0)
		return qs2fsal_error(rc);

	return fsal_status;
}

/**
 * @brief Freshen and return attributes
 *
 * This function freshens and returns the attributes of the given
 * file.
 *
 * @param[in]  obj_hdl Object to interrogate
 *
 * @return FSAL status.
 */
static fsal_status_t qs_fsal_getattrs(struct fsal_obj_handle *obj_hdl,
                                      struct attrlist *attrs)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p =====================",
	        __func__, obj_hdl);
	int rc;
	struct stat st;

	struct qs_fsal_export *export =
	    container_of(op_ctx->fsal_export, struct qs_fsal_export, export);

	struct qs_fsal_handle *handle = container_of(obj_hdl, struct qs_fsal_handle,
	                                handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter obj_hdl %p", __func__, obj_hdl);

	rc = qingstor_getattr(export->qs_fs, handle->qs_fh, &st, QS_GETATTR_FLAG_NONE);

	if (rc < 0) {
		if (attrs->request_mask & ATTR_RDATTR_ERR) {
			/* Caller asked for error to be visible. */
			attrs->valid_mask = ATTR_RDATTR_ERR;
		}
		return qs2fsal_error(rc);
	}

	posix2fsal_attributes_all(&st, attrs);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/**
 * @brief Rename a file
 *
 * This function renames a file, possibly moving it into another
 * directory.  We assume most checks are done by the caller.
 *
 * @param[in] olddir_hdl Source directory
 * @param[in] old_name   Original name
 * @param[in] newdir_hdl Destination directory
 * @param[in] new_name   New name
 *
 * @return FSAL status.
 */

static fsal_status_t qs_fsal_rename(struct fsal_obj_handle *obj_hdl,
                                    struct fsal_obj_handle *olddir_hdl,
                                    const char *old_name,
                                    struct fsal_obj_handle *newdir_hdl,
                                    const char *new_name)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p, olddir_hdl: %p,  old_name: %s , newdir_hdl:: %p, new_name: %s=====================",
	        __func__, obj_hdl, olddir_hdl, old_name, newdir_hdl, new_name);

	int rc;

	struct qs_fsal_export *export =
	    container_of(op_ctx->fsal_export, struct qs_fsal_export, export);

	struct qs_fsal_handle *olddir = container_of(obj_hdl, struct qs_fsal_handle,
	                                handle);

	struct qs_fsal_handle *newdir = container_of(obj_hdl, struct qs_fsal_handle,
	                                handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter obj_hdl %p olddir_hdl %p oname %s newdir_hdl %p nname %s",
	             __func__, obj_hdl, olddir_hdl, old_name, newdir_hdl, new_name);

	rc = qingstor_rename(export->qs_fs, olddir->qs_fh, old_name,
	                     newdir->qs_fh, new_name, QS_RENAME_FLAG_NONE);
	if (rc < 0)
		return qs2fsal_error(rc);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}



/**
 * @brief Give a hash key for file handle
 *
 * This function locates a unique hash key for a given file.
 *
 * @param[in]  obj_hdl The file whose key is to be found
 * @param[out] fh_desc    Address and length of key
 */

static void handle_to_key(struct fsal_obj_handle *obj_hdl,
                          struct gsh_buffdesc *fh_desc)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p =====================",
	        __func__, obj_hdl);

	/* The private 'full' object handle */
	struct qs_fsal_handle *handle = container_of(obj_hdl, struct qs_fsal_handle,
	                                handle);

	fh_desc->addr = &(handle->qs_fh->fh_hk);
	fh_desc->len = sizeof(struct qingstor_fh_hk);
}



/**
 * @brief Open a file descriptor for read or write and possibly create
 *
 * This function opens a file for read or write, possibly creating it.
 * If the caller is passing a state, it must hold the state_lock
 * exclusive.
 *
 * state can be NULL which indicates a stateless open (such as via the
 * NFS v3 CREATE operation), in which case the FSAL must assure protection
 * of any resources. If the file is being created, such protection is
 * simple since no one else will have access to the object yet, however,
 * in the case of an exclusive create, the common resources may still need
 * protection.
 *
 * If Name is NULL, obj_hdl is the file itself, otherwise obj_hdl is the
 * parent directory.
 *
 * On an exclusive create, the upper layer may know the object handle
 * already, so it MAY call with name == NULL. In this case, the caller
 * expects just to check the verifier.
 *
 * On a call with an existing object handle for an UNCHECKED create,
 * we can set the size to 0.
 *
 * If attributes are not set on create, the FSAL will set some minimal
 * attributes (for example, mode might be set to 0600).
 *
 * If an open by name succeeds and did not result in Ganesha creating a file,
 * the caller will need to do a subsequent permission check to confirm the
 * open. This is because the permission attributes were not available
 * beforehand.
 *
 * @param[in] obj_hdl               File to open or parent directory
 * @param[in,out] state             state_t to use for this operation
 * @param[in] openflags             Mode for open
 * @param[in] createmode            Mode for create
 * @param[in] name                  Name for file if being created or opened
 * @param[in] attrib_set            Attributes to set on created file
 * @param[in] verifier              Verifier to use for exclusive create
 * @param[in,out] new_obj           Newly created object
 * @param[in,out] caller_perm_check The caller must do a permission check
 *
 * @return FSAL status.
 */

fsal_status_t qs_fsal_open2(struct fsal_obj_handle *obj_hdl,
                            struct state_t *state,
                            fsal_openflags_t openflags,
                            enum fsal_create_mode createmode,
                            const char *name,
                            struct attrlist *attrib_set,
                            fsal_verifier_t verifier,
                            struct fsal_obj_handle **new_obj,
                            struct attrlist *attrs_out,
                            bool *caller_perm_check)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p ,name:%p =====================",
	        __func__, obj_hdl, name);

	fsal_status_t status = fsalstat(ERR_FSAL_NO_ERROR, 0);
	return status;
}



///////////////////////////////////////////////////////////////////////////////






/**
 * @brief Set attributes on an object
 *
 * This function sets attributes on an object.  Which attributes are
 * set is determined by attrib_set->valid_mask. The FSAL must manage bypass
 * or not of share reservations, and a state may be passed.
 *
 * @param[in] obj_hdl    File on which to operate
 * @param[in] state      state_t to use for this operation
 * @param[in] attrib_set Attributes to set
 *
 * @return FSAL status.
 */
fsal_status_t qs_fsal_setattr2(struct fsal_obj_handle *obj_hdl,
                               bool bypass,
                               struct state_t *state,
                               struct attrlist *attrib_set)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p =====================",
	        __func__, obj_hdl);

	fsal_status_t status = {0, 0};

	status = fsalstat(ERR_FSAL_NO_ERROR, 0);
	return status;
}
/*
int rc = 0;
bool has_lock = false;
bool closefd = false;
struct stat st;
// Mask of attributes to set
uint32_t mask = 0;


struct rgw_export *export =
    container_of(op_ctx->fsal_export, struct rgw_export, export);

struct rgw_handle *handle = container_of(obj_hdl, struct rgw_handle,
                            handle);
LogFullDebug(COMPONENT_FSAL,
             "%s enter obj_hdl %p state %p", __func__, obj_hdl, state);

if (attrib_set->valid_mask & ~RGW_SETTABLE_ATTRIBUTES) {
	LogDebug(COMPONENT_FSAL,
	         "bad mask %"PRIx64" not settable %"PRIx64,
	         attrib_set->valid_mask,
	         attrib_set->valid_mask & ~RGW_SETTABLE_ATTRIBUTES);
	return fsalstat(ERR_FSAL_INVAL, 0);
}

LogAttrlist(COMPONENT_FSAL, NIV_FULL_DEBUG,
            "attrs ", attrib_set, false);

// apply umask, if mode attribute is to be changed
if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MODE))
	attrib_set->mode &=
	    ~op_ctx->fsal_export->exp_ops.fs_umask(
	        op_ctx->fsal_export);

// Test if size is being set, make sure file is regular and if so,
// require a read/write file descriptor.
//
if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_SIZE)) {
	if (obj_hdl->type != REGULAR_FILE) {
		LogFullDebug(COMPONENT_FSAL,
		             "Setting size on non-regular file");
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	}

	// We don't actually need an open fd, we are just doing the
	// share reservation checking, thus the NULL parameters.
	//
status = fsal_find_fd(NULL, obj_hdl, NULL, &handle->share,
                      bypass, state, FSAL_O_RDWR, NULL, NULL,
                      &has_lock, &closefd, false);

if (FSAL_IS_ERROR(status)) {
	LogFullDebug(COMPONENT_FSAL,
	             "fsal_find_fd status=%s",
	             fsal_err_txt(status));
	goto out;
}
}

memset(&st, 0, sizeof(struct stat));

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_SIZE)) {
rc = rgw_truncate(export->rgw_fs, handle->rgw_fh,
                  attrib_set->filesize, RGW_TRUNCATE_FLAG_NONE);

if (rc < 0) {
	status = rgw2fsal_error(rc);
	LogDebug(COMPONENT_FSAL,
	         "truncate returned %s (%d)",
	         strerror(-rc), -rc);
	goto out;
}
}

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MODE)) {
mask |= RGW_SETATTR_MODE;
st.st_mode = fsal2unix_mode(attrib_set->mode);
}

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_OWNER)) {
mask |= RGW_SETATTR_UID;
st.st_uid = attrib_set->owner;
}

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_GROUP)) {
mask |= RGW_SETATTR_GID;
st.st_gid = attrib_set->group;
}

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_ATIME)) {
mask |= RGW_SETATTR_ATIME;
st.st_atim = attrib_set->atime;
}

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_ATIME_SERVER)) {
mask |= RGW_SETATTR_ATIME;
struct timespec timestamp;

rc = clock_gettime(CLOCK_REALTIME, &timestamp);
if (rc != 0) {
	LogDebug(COMPONENT_FSAL,
	         "clock_gettime returned %s (%d)",
	         strerror(-rc), -rc);
	status = rgw2fsal_error(rc);
	goto out;
}
st.st_atim = timestamp;
}

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MTIME)) {
mask |= RGW_SETATTR_MTIME;
st.st_mtim = attrib_set->mtime;
}
if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MTIME_SERVER)) {
mask |= RGW_SETATTR_MTIME;
struct timespec timestamp;

rc = clock_gettime(CLOCK_REALTIME, &timestamp);
if (rc != 0) {
	LogDebug(COMPONENT_FSAL,
	         "clock_gettime returned %s (%d)",
	         strerror(-rc), -rc);
	status = rgw2fsal_error(rc);
	goto out;
}
st.st_mtim = timestamp;
}

if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_CTIME)) {
mask |= RGW_SETATTR_CTIME;
st.st_ctim = attrib_set->ctime;
}

rc = rgw_setattr(export->rgw_fs, handle->rgw_fh, &st, mask,
             RGW_SETATTR_FLAG_NONE);

if (rc < 0) {
LogDebug(COMPONENT_FSAL,
         "setattr returned %s (%d)",
         strerror(-rc), -rc);

status = rgw2fsal_error(rc);
} else {
// Success
status = fsalstat(ERR_FSAL_NO_ERROR, 0);
//}

//out:

//if (has_lock)
//PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);

return status;
}




* @brief Remove a name
*
* This function removes a name from the filesystem and possibly
* deletes the associated file.  Directories must be empty to be
* removed.
*
* @param[in] dir_hdl The directory from which to remove the name
* @param[in] obj_hdl The object being removed
* @param[in] name    The name to remove
*
* @return FSAL status.

static fsal_status_t qs_fsal_unlink(struct fsal_obj_handle *dir_hdl,
				struct fsal_obj_handle *obj_hdl,
				const char *name)
{
	int rc;

	struct rgw_export *export =
		container_of(op_ctx->fsal_export, struct rgw_export, export);

	struct rgw_handle *dir = container_of(dir_hdl, struct rgw_handle,
					handle);

	LogFullDebug(COMPONENT_FSAL,
		"%s enter dir_hdl %p obj_hdl %p name %s", __func__, dir_hdl,
		obj_hdl, name);

	rc = rgw_unlink(export->rgw_fs, dir->rgw_fh, name,
			RGW_UNLINK_FLAG_NONE);
	if (rc < 0)
		return rgw2fsal_error(rc);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}
*/
/**
 * @brief Merge a duplicate handle with an original handle
 *
 * This function is used if an upper layer detects that a duplicate
 * object handle has been created. It allows the FSAL to merge anything
 * from the duplicate back into the original.
 *
 * The caller must release the object (the caller may have to close
 * files if the merge is unsuccessful).
 *
 * @param[in]  orig_hdl  Original handle
 * @param[in]  dupe_hdl Handle to merge into original
 *
 * @return FSAL status.
 *
 */

fsal_status_t qs_fsal_merge(struct fsal_obj_handle * orig_hdl,
                            struct fsal_obj_handle * dupe_hdl)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p =====================",
	        __func__, orig_hdl);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};

	/*
	if (orig_hdl->type == REGULAR_FILE &&
	        dupe_hdl->type == REGULAR_FILE) {
		// We need to merge the share reservations on this file.
		//This could result in ERR_FSAL_SHARE_DENIED.

		struct rgw_handle *orig, *dupe;

		orig = container_of(orig_hdl, struct rgw_handle, handle);
		dupe = container_of(dupe_hdl, struct rgw_handle, handle);

		// This can block over an I/O operation.
		PTHREAD_RWLOCK_wrlock(&orig_hdl->obj_lock);

		status = merge_share(&orig->share, &dupe->share);

		PTHREAD_RWLOCK_unlock(&orig_hdl->obj_lock);
	}
	*/

	return status;
}

/**
 * @brief Return open status of a state.
 *
 * This function returns open flags representing the current open
 * status for a state. The state_lock must be held.
 *
 * @param[in] obj_hdl     File on which to operate
 * @param[in] state       File state to interrogate
 *
 * @retval Flags representing current open status
 */

fsal_openflags_t qs_fsal_status2(struct fsal_obj_handle * obj_hdl,
                                 struct state_t *state)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p =====================",
	        __func__, obj_hdl);

	struct qs_fsal_handle *handle = container_of(obj_hdl, struct qs_fsal_handle,
	                                handle);

	/* normal FSALs recover open state in "state" */

	return handle->openflags;
}

/**
 * @brief Re-open a file that may be already opened
 *
 * This function supports changing the access mode of a share reservation and
 * thus should only be called with a share state. The state_lock must be held.
 *
 * This MAY be used to open a file the first time if there is no need for
 * open by name or create semantics. One example would be 9P lopen.
 *
 * @param[in] obj_hdl     File on which to operate
 * @param[in] state       state_t to use for this operation
 * @param[in] openflags   Mode for re-open
 *
 * @return FSAL status.
 */
fsal_status_t qs_fsal_reopen2(struct fsal_obj_handle * obj_hdl,
                              struct state_t *state,
                              fsal_openflags_t openflags)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p =====================",
	        __func__, obj_hdl);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	return status;

}
/*
fsal_status_t rgw_fsal_reopen2(struct fsal_obj_handle * obj_hdl,
                               struct state_t *state,
                               fsal_openflags_t openflags)
{
	fsal_status_t status = {0, 0};
	int posix_flags = 0;
	fsal_openflags_t old_openflags;
	struct rgw_open_state *open_state = NULL;

	struct rgw_export *export =
	    container_of(op_ctx->fsal_export, struct rgw_export, export);

	struct rgw_handle *handle = container_of(obj_hdl, struct rgw_handle,
	                            handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter obj_hdl %p state %p", __func__, obj_hdl, open_state);

	// RGW fsal does not permit concurrent opens, so openflags
	// are recovered from handle

	if (state) {
		//a conceptual open state exists
		open_state = (struct rgw_open_state *) state;
		LogFullDebug(COMPONENT_FSAL,
		             "%s called w/open_state %p", __func__, open_state);
	}

	fsal2posix_openflags(openflags, &posix_flags);

	// This can block over an I/O operation.
	PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

	old_openflags = handle->openflags;

	// We can conflict with old share, so go ahead and check now.
	status = check_share_conflict(&handle->share, openflags, false);

	if (FSAL_IS_ERROR(status)) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);

		return status;
	}

	// Set up the new share so we can drop the lock and not have a
	// conflicting share be asserted, updating the share counters.
	//
	update_share_counters(&handle->share, old_openflags, openflags);

	PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);

	// perform a provider open iff not already open
if (!fsal_is_open(obj_hdl)) {

	// XXX alo, how do we know the ULP tracks opens?
	// 9P does, V3 does not

	int rc = rgw_open(export->rgw_fs, handle->rgw_fh,
	                  posix_flags,
	                  (!state) ? RGW_OPEN_FLAG_V3 :
	                  RGW_OPEN_FLAG_NONE);

	if (rc < 0) {
		// We had a failure on open - we need to revert the
		// share. This can block over an I/O operation.
		//
		PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

		update_share_counters(&handle->share, openflags,
		                      old_openflags);

		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	status = rgw2fsal_error(rc);
}

return status;
}*/

/**
 * @brief Read data from a file
 *
 * This function reads data from the given file. The FSAL must be able to
 * perform the read whether a state is presented or not. This function also
 * is expected to handle properly bypassing or not share reservations.
 *
 * @param[in]     obj_hdl        File on which to operate
 * @param[in]     bypass         If state doesn't indicate a share reservation,
 *                               bypass any deny read
 * @param[in]     state          state_t to use for this operation
 * @param[in]     offset         Position from which to read
 * @param[in]     buffer_size    Amount of data to read
 * @param[out]    buffer         Buffer to which data are to be copied
 * @param[out]    read_amount    Amount of data read
 * @param[out]    end_of_file    true if the end of file has been reached
 * @param[in,out] info           more information about the data
 *
 * @return FSAL status.
 */

fsal_status_t qs_fsal_read2(struct fsal_obj_handle * obj_hdl,
                            bool bypass,
                            struct state_t *state,
                            uint64_t offset,
                            size_t buffer_size,
                            void *buffer,
                            size_t *read_amount,
                            bool * end_of_file,
                            struct io_info * info)
{

	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p ,offset : %d, buffer_size: %d=====================",
	        __func__, obj_hdl, (int)offset, (int)buffer_size);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	return status;

}
/*
	{
		struct rgw_export *export =
		    container_of(op_ctx->fsal_export, struct rgw_export, export);

		struct rgw_handle *handle = container_of(obj_hdl, struct rgw_handle,
		                            handle);

		LogFullDebug(COMPONENT_FSAL,
		             "%s enter obj_hdl %p state %p", __func__, obj_hdl, state);

		if (info != NULL) {
			// Currently we don't support READ_PLUS
			return fsalstat(ERR_FSAL_NOTSUPP, 0);
		}

		// QingStor does not support a file descriptor abstraction--so
		// reads are handle based

		int rc = rgw_read(export->rgw_fs, handle->rgw_fh, offset,
		                  buffer_size, read_amount, buffer,
		                  RGW_READ_FLAG_NONE);

		if (rc < 0)
			return rgw2fsal_error(rc);

		*end_of_file = (read_amount == 0);

		return fsalstat(ERR_FSAL_NO_ERROR, 0);
	}
*/
/**
 * @brief Write data to a file
 *
 * This function writes data to a file. The FSAL must be able to
 * perform the write whether a state is presented or not. This function also
 * is expected to handle properly bypassing or not share reservations. Even
 * with bypass == true, it will enforce a mandatory (NFSv4) deny_write if
 * an appropriate state is not passed).
 *
 * The FSAL is expected to enforce sync if necessary.
 *
 * @param[in]     obj_hdl        File on which to operate
 * @param[in]     bypass         If state doesn't indicate a share reservation,
 *                               bypass any non-mandatory deny write
 * @param[in]     state          state_t to use for this operation
 * @param[in]     offset         Position at which to write
 * @param[in]     buffer         Data to be written
 * @param[in,out] fsal_stable    In, if on, the fsal is requested to write data
 *                               to stable store. Out, the fsal reports what
 *                               it did.
 * @param[in,out] info           more information about the data
 *
 * @return FSAL status.
 */

fsal_status_t qs_fsal_write2(struct fsal_obj_handle * obj_hdl,
                             bool bypass,
                             struct state_t *state,
                             uint64_t offset,
                             size_t buffer_size,
                             void *buffer,
                             size_t *wrote_amount,
                             bool * fsal_stable,
                             struct io_info * info)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p ,offset : %d, buffer_size: %d=====================",
	        __func__, obj_hdl, (int)offset, (int)buffer_size);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	return status;

}
/*
	fsal_status_t rgw_fsal_write2(struct fsal_obj_handle * obj_hdl,
	                              bool bypass,
	                              struct state_t *state,
	                              uint64_t offset,
	                              size_t buffer_size,
	                              void *buffer,
	                              size_t *wrote_amount,
	                              bool * fsal_stable,
	                              struct io_info * info)
	{
		struct rgw_export *export =
		    container_of(op_ctx->fsal_export, struct rgw_export, export);

		struct rgw_handle *handle = container_of(obj_hdl, struct rgw_handle,
		                            handle);
		LogFullDebug(COMPONENT_FSAL,
		             "%s enter obj_hdl %p state %p", __func__, obj_hdl, state);

		if (info != NULL) {
			// Currently we don't support WRITE_PLUS
			return fsalstat(ERR_FSAL_NOTSUPP, 0);
		}

		// XXX note no call to fsal_find_fd (or wrapper)

		int rc = rgw_write(export->rgw_fs, handle->rgw_fh, offset,
		                   buffer_size, wrote_amount, buffer,
		                   RGW_WRITE_FLAG_NONE);

		LogFullDebug(COMPONENT_FSAL,
		             "%s post obj_hdl %p state %p returned %d", __func__, obj_hdl,
		             state, rc);

		if (rc < 0)
			return rgw2fsal_error(rc);

		if (*fsal_stable) {
			rc = rgw_fsync(export->rgw_fs, handle->rgw_fh,
			               RGW_WRITE_FLAG_NONE);
			if (rc < 0)
				return rgw2fsal_error(rc);
		}

		return fsalstat(ERR_FSAL_NO_ERROR, 0);
	}
*/
/**
 * @brief Commit written data
 *
 * This function flushes possibly buffered data to a filegfew`. This method
 * differs from commit due to the need to interact with share reservations
 * and the fact that the FSAL manages the state of "file descriptors". The
 * FSAL must be able to perform this operation without being passed a specific
 * state.
 *
 * @param[in] obj_hdl          File on which to operate
 * @param[in] state            state_t to use for this operation
 * @param[in] offset           Start of range to commit
 * @param[in] len              Length of range to commit
 *
 * @return FSAL status.
 */
fsal_status_t qs_fsal_commit2(struct fsal_obj_handle * obj_hdl,
                              off_t offset, size_t length)
{

	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p ,offset : %d, length: %d=====================",
	        __func__, obj_hdl,  (int)offset, (int)length);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	return status;

}
/*
fsal_status_t rgw_fsal_commit2(struct fsal_obj_handle * obj_hdl,
                               off_t offset, size_t length)
{
	int rc;

	struct rgw_export *export =
	    container_of(op_ctx->fsal_export, struct rgw_export, export);

	struct rgw_handle *handle = container_of(obj_hdl, struct rgw_handle,
	                            handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter obj_hdl %p offset %"PRIx64" length %zx",
	             __func__, obj_hdl, (uint64_t) offset, length);

	rc = rgw_commit(export->rgw_fs, handle->rgw_fh, offset, length,
	                RGW_FSYNC_FLAG_NONE);
	if (rc < 0)
		return rgw2fsal_error(rc);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}
*/
/**
 * @brief Allocate a state_t structure
 *
 * Note that this is not expected to fail since memory allocation is
 * expected to abort on failure.
 *
 * @param[in] exp_hdl               Export state_t will be associated with
 * @param[in] state_type            Type of state to allocate
 * @param[in] related_state         Related state if appropriate
 *
 * @returns a state structure.
 */

struct state_t *qs_alloc_state(struct fsal_export * exp_hdl,
                               enum state_type state_type,
                               struct state_t *related_state)
{

	LogCrit(COMPONENT_FSAL,
	        "=================== %s with exp_hdl: %p =====================",
	        __func__, exp_hdl);
		
	return init_state(gsh_calloc(1, sizeof(struct qs_fsal_open_state)),
			exp_hdl, state_type, related_state);
}
/*
struct state_t *rgw_alloc_state(struct fsal_export * exp_hdl,
                                enum state_type state_type,
                                struct state_t *related_state)
{
	return init_state(gsh_calloc(1, sizeof(struct rgw_open_state)),
	                  exp_hdl, state_type, related_state);
}
*/
/**
 * @brief Manage closing a file when a state is no longer needed.
 *
 * When the upper layers are ready to dispense with a state, this method is
 * called to allow the FSAL to close any file descriptors or release any other
 * resources associated with the state. A call to free_state should be assumed
 * to follow soon.
 *
 * @param[in] obj_hdl    File on which to operate
 * @param[in] state      state_t to use for this operation
 *
 * @return FSAL status.
 */
fsal_status_t qs_fsal_close2(struct fsal_obj_handle * obj_hdl,
                             struct state_t *state)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with obj_hdl: %p =====================",
	        __func__, obj_hdl);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	return status;
}
/*
fsal_status_t qs_fsal_close2(struct fsal_obj_handle * obj_hdl,
                             struct state_t *state)
{
	int rc;
	struct rgw_open_state *open_state;

	struct rgw_export *export =
	    container_of(op_ctx->fsal_export, struct rgw_export, export);

	struct rgw_handle *handle = container_of(obj_hdl, struct rgw_handle,
	                            handle);

	LogFullDebug(COMPONENT_FSAL,
	             "%s enter obj_hdl %p state %p", __func__, obj_hdl, state);

	if (state) {
		open_state = (struct rgw_open_state *) state;

		LogFullDebug(COMPONENT_FSAL,
		             "%s called w/open_state %p", __func__, open_state);

		if (state->state_type == STATE_TYPE_SHARE ||
		        state->state_type == STATE_TYPE_NLM_SHARE ||
		        state->state_type == STATE_TYPE_9P_FID) {
			// This is a share state, we must update the share
			// counters.  This can block over an I/O operation.
			//
			PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

			update_share_counters(&handle->share,
			                      handle->openflags,
			                      FSAL_O_CLOSED);

			PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
		}
	}

	rc = rgw_close(export->rgw_fs, handle->rgw_fh, RGW_CLOSE_FLAG_NONE);
	if (rc < 0)
		return rgw2fsal_error(rc);

	handle->openflags = FSAL_O_CLOSED;

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}
*/

/**
 * @brief Close the global FD for a file
 *
 * This function closes a file, freeing resources used for read/write
 * access and releasing capabilities.
  *
 * @param[in] handle_pub File to close
  *
  * @return FSAL status.
  */
fsal_status_t qs_fsal_close(struct fsal_obj_handle * handle_pub)
{
	LogCrit(COMPONENT_FSAL,
	        "=================== %s with handle_pub: %p =====================",
	        __func__, handle_pub);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	return status;
}
	
/*
static fsal_status_t rgw_fsal_close(struct fsal_obj_handle * handle_pub)
{

	return rgw_fsal_close2(handle_pub, NULL);
}

*/

/**
 * @brief Write wire handle
 *
 * This function writes a 'wire' handle to be sent to clients and
 * received from the.
 *
 * @param[in]     obj_hdl  Handle to digest
 * @param[in]     output_type Type of digest requested
 * @param[in,out] fh_desc     Location/size of buffer for
 *                            digest/Length modified to digest length
 *
 * @return FSAL status.
 */

static fsal_status_t handle_to_wire(const struct fsal_obj_handle * obj_hdl,
                                    uint32_t output_type,
                                    struct gsh_buffdesc * fh_desc)
{
	/* The private 'full' object handle */
	const struct qs_fsal_handle *handle =
	    container_of(obj_hdl, const struct qs_fsal_handle, handle);

	switch (output_type) {
	/* Digested Handles */
	case FSAL_DIGEST_NFSV3:
	case FSAL_DIGEST_NFSV4:
		if (fh_desc->len < sizeof(struct qingstor_fh_hk)) {
			LogMajor(COMPONENT_FSAL,
			         "QingStor digest_handle: space too small for handle.  Need %zu, have %zu",
			         sizeof(handle->qs_fh), fh_desc->len);
			return fsalstat(ERR_FSAL_TOOSMALL, 0);
		} else {
			memcpy(fh_desc->addr, &(handle->qs_fh->fh_hk),
			       sizeof(struct qingstor_fh_hk));
			fh_desc->len = sizeof(struct qingstor_fh_hk);
		}
		break;

	default:
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}


/**
 * @brief Override functions in ops vector
 *
 * This function overrides implemented functions in the ops vector
 * with versions for this FSAL.
 *
 * @param[in] ops Handle operations vector
 */

// 均是对文件/目录object的操作
void handle_ops_init(struct fsal_obj_ops * ops)
{
	ops->release = release;
	ops->merge = qs_fsal_merge;
	ops->lookup = lookup;
	ops->create = qs_fsal_create;
	ops->mkdir = qs_fsal_mkdir;
	ops->readdir = qs_fsal_readdir;
	ops->getattrs = qs_fsal_getattrs;
	ops->rename = qs_fsal_rename;
	//ops->unlink = rgw_fsal_unlink;
	ops->close = qs_fsal_close;
	ops->handle_to_wire = handle_to_wire;
	ops->handle_to_key = handle_to_key;
	ops->open2 = qs_fsal_open2;
	ops->status2 = qs_fsal_status2;
	ops->reopen2 = qs_fsal_reopen2;
	ops->read2 = qs_fsal_read2;
	ops->write2 = qs_fsal_write2;
	ops->commit2 = qs_fsal_commit2;
	ops->setattr2 = qs_fsal_setattr2;
	ops->close2 = qs_fsal_close2;
}
