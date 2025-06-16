//
// Created by ryuzot on 25/01/02.
//

#ifndef MESHID_OPS_H
#define MESHID_OPS_H

#define _GNU_SOURCE

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <cmph.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <limits.h>

#ifdef __cplusplus
extern "C" {
#endif

extern const size_t meshid_list_size;
extern const uint32_t meshid_list[];
extern unsigned char _binary_meshid_mobaku_mph_start[];
extern unsigned char _binary_meshid_mobaku_mph_end[];
extern unsigned char _binary_meshid_mobaku_mph_size[];

#ifdef __cplusplus
}
#endif

// Time-related constants
#define REFERENCE_MOBAKU_DATETIME "2016-01-01 00:00:00"
static const time_t REFERENCE_MOBAKU_TIME = 1451574000;
static const int64_t POSTGRES_EPOCH_IN_UNIX = 946684800LL;
static const int64_t JST_OFFSET_SEC = 9 * 3600;

// Dataset dimensions - updated to match HDF5 specification
#define NOW_ENTIRE_LEN_FOR_ONE_MESH 74160
#define HDF5_DATETIME_CHUNK 8784
#define HDF5_MESH_CHUNK 16
#define NUM_PRODUCERS 32
#define MESHLIST_ONCE_LEN 16
#define QUEUE_SIZE 1024
#define NUM_MESHES_1ST 25600

// Legacy constants for backward compatibility
#define MOBAKU_TIME_POINTS 74160
#define MOBAKU_MESH_COUNT 1553332

// Special mesh ID handling
#define SPECIAL_MESH_ID 684827214
#define SPECIAL_MESH_INDEX 1553331

time_t meshid_pg_bin_timestamp_to_jst(const char *bin_ptr, int len);

int meshid_get_time_index_from_datetime(char *now_time_str);

int meshid_get_time_index_from_time(time_t now_time);

char* meshid_get_datetime_from_time_index(int time_index);

void meshid_uint_to_str(unsigned int num, char *str);

// 検索準備関数
cmph_t* meshid_prepare_search(void);

// 検索関数
#define MESHID_NOT_FOUND UINT32_MAX  // Error return value for search_id
uint32_t meshid_search_id(cmph_t *hash, uint32_t key);

char** meshid_uint_array_to_string_array(const int* int_array, size_t nkeys);

// 文字列配列を解放する関数
void meshid_free_string_array(char** str_array, size_t nkeys);

// ハッシュ関数を作成する関数
cmph_t* meshid_create_local_mph_from_int(int* int_array, size_t nkeys);

int meshid_find_local_id(cmph_t* hash, uint32_t key);

void meshid_print_progress_bar(int now, int all);

int* meshid_get_all_meshes_in_1st_mesh(int meshid_1, int num_meshes);


#endif //MESHID_OPS_H
