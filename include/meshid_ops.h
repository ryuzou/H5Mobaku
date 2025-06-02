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

#define REFERENCE_MOBAKU_DATETIME "2016-01-01 00:00:00"
static const time_t REFERENCE_MOBAKU_TIME = 1451574000;

static const int64_t POSTGRES_EPOCH_IN_UNIX = 946684800LL;

// AWARE !!! HARD CODED !!!
static const int64_t JST_OFFSET_SEC = 9 * 3600;

time_t pg_bin_timestamp_to_jst(const char *bin_ptr, int len);

int get_time_index_mobaku_datetime(char *now_time_str);

int get_time_index_mobaku_datetime_from_time(time_t now_time);

char* get_mobaku_datetime_from_time_index(int time_index);

void uint2str(unsigned int num, char *str);

// 検索準備関数
cmph_t* prepare_search(void);

// 検索関数
uint32_t search_id(cmph_t *hash, uint32_t key);

char** uint_array_to_string_array(const int* int_array, size_t nkeys);

// 文字列配列を解放する関数
void free_string_array(char** str_array, size_t nkeys);

// ハッシュ関数を作成する関数
cmph_t* create_local_mph_from_int(int* int_array, size_t nkeys);

int find_local_id(cmph_t* hash, uint32_t key);

void printProgressBar(int now, int all);

int* get_all_meshes_in_1st_mesh(int meshid_1, int NUM_MESHES);

// HDF5 population data reading functions
struct h5r;  // Forward declaration

// Read population data for a single mesh at a specific time index
int32_t read_population_single(struct h5r *h5_ctx, uint32_t mesh_id, int time_index);

// Read population data for multiple meshes at a specific time index
int32_t* read_population_multi(struct h5r *h5_ctx, uint32_t *mesh_ids, size_t num_meshes, int time_index);

// Read population time series for a single mesh
int32_t* read_population_time_series(struct h5r *h5_ctx, uint32_t mesh_id, int start_time_index, int end_time_index);

// Read population data for hierarchical mesh aggregation
int64_t read_population_hierarchical(struct h5r *h5_ctx, const char *mesh_prefix, int time_index);

// Read population data for a rectangular region in the dataset
int32_t* read_population_rect(struct h5r *h5_ctx, int start_time_index, int end_time_index, uint32_t *mesh_ids, size_t num_meshes);

#endif //MESHID_OPS_H
