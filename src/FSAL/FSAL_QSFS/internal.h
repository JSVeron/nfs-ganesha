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
/**
 * @file   internal.h
 * @brief Internal declarations for the QINGSTOR FSAL
 *
 * This file includes declarations of data types, functions,
 * variables, and constants for the QINGSTOR FSAL.
 */

#ifndef FSAL_QINGSTOR_INTERNAL_INTERNAL
#define FSAL_QINGSTOR_INTERNAL_INTERNAL

#include "fsal.h"
#include "fsal_types.h"
#include "fsal_api.h"
#include "fsal_convert.h"
#include "sal_data.h"


/**
 * QINGSTOR Main (global) module object
 */
struct qs_fsal_module {
	struct fsal_module fsal;
	fsal_staticfsinfo_t fs_info;
	char *conf_path;
	char *name;
	//char *cluster;
	char *init_args;
	//librgw_t rgw;
	libqs_t libqsfs;
};

extern struct qs_fsal_module QSFSM;

#define MAXUIDLEN 32
#define MAXKEYLEN 20
#define MAXSECRETLEN 40

/**
 * QINGSTOR internal export object
 */

struct qs_fsal_export {
	struct fsal_export export;	/*< The public export object */
	struct qingstor_file_system *qs_fs;		/*< "Opaque" fs handle */
	struct qs_fsal_handle *root;    /*< root handle */
	char *qs_fsal_name;
	char *qs_fsal_user_id;
	char *qs_fsal_bucket_name;
	char *qs_fsal_zone;
	//char *qs_fsal_access_key_id;
	//char *qs_fsal_secret_access_key;
};

/**
 * The QINGSTOR FSAL internal handle
 */

struct qs_fsal_handle {
	struct fsal_obj_handle handle;	/*< The public handle */
	struct qingstor_file_handle *qs_fh;  /*< QINGSTOR-internal file handle */
	/* XXXX remove ptr to up-ops--we can always follow export! */
	const struct fsal_up_vector *up_ops;	/*< Upcall operations */
	struct qs_fsal_export *export;	/*< The first export this handle
					 *< belongs to */
	struct fsal_share share;
	fsal_openflags_t openflags;
};

/**
 * QingStor "file descriptor"
 */
struct qs_fsal_open_state {
	struct state_t gsh_open;
	uint32_t flags;
};

/**
 * The attributes this FSAL can interpret or supply.
 * Currently FSAL_RGW uses posix2fsal_attributes, so we should indicate support
 * for at least those attributes.
 */
#define QINGSTOR_SUPPORTED_ATTRIBUTES ((const attrmask_t) (ATTRS_POSIX))


/* Prototypes */
int construct_handle(struct qs_fasl_export *export,
                     struct qingstor_file_handle *rgw_file_handle,
                     struct stat *st,
                     struct qs_fsal_handle **obj);

void deconstruct_handle(struct qs_fsal_handle *obj);

fsal_status_t qs2fsal_error(const int errorcode);