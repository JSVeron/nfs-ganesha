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

#include "internal.h"

struct qs_fsal_module QSFSM;

/**
 * @brief FSAL status from RGW error
 *
 * This function returns a fsal_status_t with the FSAL error as the
 * major, and the posix error as minor.	 (RGW's error codes are just
 * negative signed versions of POSIX error codes.)
 *
 * @param[in] rgw_errorcode RGW error (negative Posix)
 *
 * @return FSAL status.
 */

fsal_status_t qs2fsal_error(const int rgw_errorcode)
{
	fsal_status_t status;
	status.minor = -rgw_errorcode;

	switch (-rgw_errorcode) {

	case 0:
		status.major = ERR_FSAL_NO_ERROR;
		break;

	case EPERM:
		status.major = ERR_FSAL_PERM;
		break;

	case ENOENT:
		status.major = ERR_FSAL_NOENT;
		break;

	case ECONNREFUSED:
	case ECONNABORTED:
	case ECONNRESET:
	case EIO:
	case ENFILE:
	case EMFILE:
	case EPIPE:
		status.major = ERR_FSAL_IO;
		break;

	case ENODEV:
	case ENXIO:
		status.major = ERR_FSAL_NXIO;
		break;

	case EBADF:
		/**
		 * @todo: The EBADF error also happens when file is
		 *	  opened for reading, and we try writting in
		 *	  it.  In this case, we return
		 *	  ERR_FSAL_NOT_OPENED, but it doesn't seems to
		 *	  be a correct error translation.
		 */
		status.major = ERR_FSAL_NOT_OPENED;
		break;

	case ENOMEM:
		status.major = ERR_FSAL_NOMEM;
		break;

	case EACCES:
		status.major = ERR_FSAL_ACCESS;
		break;

	case EFAULT:
		status.major = ERR_FSAL_FAULT;
		break;

	case EEXIST:
		status.major = ERR_FSAL_EXIST;
		break;

	case EXDEV:
		status.major = ERR_FSAL_XDEV;
		break;

	case ENOTDIR:
		status.major = ERR_FSAL_NOTDIR;
		break;

	case EISDIR:
		status.major = ERR_FSAL_ISDIR;
		break;

	case EINVAL:
		status.major = ERR_FSAL_INVAL;
		break;

	case EFBIG:
		status.major = ERR_FSAL_FBIG;
		break;

	case ENOSPC:
		status.major = ERR_FSAL_NOSPC;
		break;

	case EMLINK:
		status.major = ERR_FSAL_MLINK;
		break;

	case EDQUOT:
		status.major = ERR_FSAL_DQUOT;
		break;

	case ENAMETOOLONG:
		status.major = ERR_FSAL_NAMETOOLONG;
		break;

	case ENOTEMPTY:
		status.major = ERR_FSAL_NOTEMPTY;
		break;

	case ESTALE:
		status.major = ERR_FSAL_STALE;
		break;

	case EAGAIN:
	case EBUSY:
		status.major = ERR_FSAL_DELAY;
		break;

	default:
		status.major = ERR_FSAL_SERVERFAULT;
		break;
	}

	return status;
}

/**
 * @brief Construct a new filehandle
 *
 * This function constructs a new QINGSTOR FSAL object handle and attaches
 * it to the export.  After this call the attributes have been filled
 * in and the handle is up-to-date and usable.
 *
 * @param[in]  export Export on which the object lives
 * @param[in]  qingstor_fh Concise representation of the object name,
 *                    in QINGSTOR notation
 * @param[inout] st   Object attributes
 * @param[out] obj    Object created
 *
 * @return 0 on success, negative error codes on failure.
 */

int construct_handle(struct qingstor_export *export,
                     struct qingstor_file_handle *qingstor_fh,
                     struct stat *st,
                     struct qs_fsal_handle **obj)

{
	/* Poitner to the handle under construction */
	struct qs_fsal_handle *constructing = NULL;
	*obj = NULL;

	constructing = gsh_calloc(1, sizeof(struct qs_fsal_handle));
	if (constructing == NULL)
		return -ENOMEM;

	constructing->qingstor_fh = qingstor_fh;
	constructing->up_ops = export->export.up_ops; /* XXXX going away */

	fsal_obj_handle_init(&constructing->handle, &export->export,
	                     posix2fsal_type(st->st_mode));
	handle_ops_init(&constructing->handle.obj_ops);
	constructing->handle.fsid = posix2fsal_fsid(st->st_dev);
	constructing->handle.fileid = st->st_ino;

	constructing->export = export;

	*obj = constructing;

	return 0;
}

void deconstruct_handle(struct qs_fsal_handle *obj)
{
	fsal_obj_handle_fini(&obj->handle);
	gsh_free(obj);
}
