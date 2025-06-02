//
// Created by ryuzot on 25/02/06.
//

#ifndef H5MOBAKU_OPS_H
#define H5MOBAKU_OPS_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include "H5MR/h5mr.h"
#include "meshid_ops.h"

#ifdef __cplusplus
extern "C" {
#endif

// H5Mobaku wrapper structure
struct h5mobaku {
    struct h5r *h5r_ctx;      // Wrapped h5r context
    time_t start_datetime;    // Start datetime from HDF5 attribute
    char *start_datetime_str; // String representation of start datetime
};

// Initialize/cleanup functions
int h5mobaku_open(const char *path, struct h5mobaku **out);
void h5mobaku_close(struct h5mobaku *ctx);

// Time-based API functions (using datetime strings)
// Read population data for a single mesh at a specific time
int32_t h5mobaku_read_population_single_at_time(struct h5mobaku *ctx, cmph_t *hash, uint32_t mesh_id, const char *datetime_str);

// Read population data for multiple meshes at a specific time
int32_t* h5mobaku_read_population_multi_at_time(struct h5mobaku *ctx, cmph_t *hash, uint32_t *mesh_ids, size_t num_meshes, const char *datetime_str);

// Read population time series for a single mesh between two times
int32_t* h5mobaku_read_population_time_series_between(struct h5mobaku *ctx, cmph_t *hash, uint32_t mesh_id, const char *start_datetime_str, const char *end_datetime_str);

// Original index-based API functions (kept for backward compatibility)
int32_t h5mobaku_read_population_single(struct h5r *h5_ctx, cmph_t *hash, uint32_t mesh_id, int time_index);
int32_t* h5mobaku_read_population_multi(struct h5r *h5_ctx, cmph_t *hash, uint32_t *mesh_ids, size_t num_meshes, int time_index);
int32_t* h5mobaku_read_population_time_series(struct h5r *h5_ctx, cmph_t *hash, uint32_t mesh_id, int start_time_index, int end_time_index);

// Free allocated memory from multi/time_series functions
void h5mobaku_free_data(int32_t *data);

#ifdef __cplusplus
}
#endif

#endif //H5MOBAKU_OPS_H