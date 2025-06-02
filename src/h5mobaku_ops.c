//
// Created by ryuzot on 25/02/06.
//

#define _GNU_SOURCE
#include "h5mobaku_ops.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <hdf5.h>
#include <time.h>

// Initialize h5mobaku wrapper
int h5mobaku_open(const char *path, struct h5mobaku **out) {
    if (!path || !out) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_open\n");
        return -1;
    }
    
    struct h5mobaku *ctx = calloc(1, sizeof(struct h5mobaku));
    if (!ctx) {
        fprintf(stderr, "Error: Memory allocation failed for h5mobaku context\n");
        return -1;
    }
    
    // Open the underlying h5r context
    int ret = h5r_open(path, &ctx->h5r_ctx);
    if (ret < 0) {
        free(ctx);
        return -1;
    }
    
    // Read the start_datetime attribute from the HDF5 file
    hid_t file_id = H5Fopen(path, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (file_id < 0) {
        h5r_close(ctx->h5r_ctx);
        free(ctx);
        return -1;
    }
    
    hid_t dset_id = H5Dopen2(file_id, "population_data", H5P_DEFAULT);
    if (dset_id < 0) {
        H5Fclose(file_id);
        h5r_close(ctx->h5r_ctx);
        free(ctx);
        return -1;
    }
    
    // Read the start_datetime attribute
    hid_t attr_id = H5Aopen(dset_id, "start_datetime", H5P_DEFAULT);
    if (attr_id >= 0) {
        hid_t atype = H5Aget_type(attr_id);
        hid_t atype_mem = H5Tget_native_type(atype, H5T_DIR_ASCEND);
        
        char *attr_value = NULL;
        herr_t status = H5Aread(attr_id, atype_mem, &attr_value);
        
        if (status >= 0 && attr_value) {
            ctx->start_datetime_str = strdup(attr_value);
            
            // Parse the datetime string to time_t
            struct tm tm = {0};
            if (strptime(ctx->start_datetime_str, "%Y-%m-%d %H:%M:%S", &tm) != NULL) {
                ctx->start_datetime = mktime(&tm);
            } else {
                // Fallback to default if parsing fails
                ctx->start_datetime = REFERENCE_MOBAKU_TIME;
            }
            
            H5free_memory(attr_value);
        } else {
            // Use default if attribute not found
            ctx->start_datetime = REFERENCE_MOBAKU_TIME;
            ctx->start_datetime_str = strdup(REFERENCE_MOBAKU_DATETIME);
        }
        
        H5Tclose(atype_mem);
        H5Tclose(atype);
        H5Aclose(attr_id);
    } else {
        // Use default if attribute not found
        ctx->start_datetime = REFERENCE_MOBAKU_TIME;
        ctx->start_datetime_str = strdup(REFERENCE_MOBAKU_DATETIME);
    }
    
    H5Dclose(dset_id);
    H5Fclose(file_id);
    
    *out = ctx;
    return 0;
}

// Cleanup h5mobaku wrapper
void h5mobaku_close(struct h5mobaku *ctx) {
    if (!ctx) return;
    
    if (ctx->h5r_ctx) {
        h5r_close(ctx->h5r_ctx);
    }
    
    if (ctx->start_datetime_str) {
        free(ctx->start_datetime_str);
    }
    
    free(ctx);
}

// Helper function to convert datetime string to time index
static int datetime_to_index(struct h5mobaku *ctx, const char *datetime_str) {
    if (!ctx || !datetime_str) return -1;
    
    struct tm tm = {0};
    if (strptime(datetime_str, "%Y-%m-%d %H:%M:%S", &tm) == NULL) {
        fprintf(stderr, "Error: Failed to parse datetime string '%s'\n", datetime_str);
        return -1;
    }
    
    time_t target_time = mktime(&tm);
    if (target_time == (time_t)-1) {
        fprintf(stderr, "Error: Failed to convert datetime to timestamp\n");
        return -1;
    }
    
    // Calculate hours difference from start datetime
    int index = (int)(difftime(target_time, ctx->start_datetime) / 3600.0);
    if (index < 0) {
        fprintf(stderr, "Error: Datetime '%s' is before start datetime '%s'\n", 
                datetime_str, ctx->start_datetime_str);
        return -1;
    }
    
    return index;
}

// Time-based wrapper functions
int32_t h5mobaku_read_population_single_at_time(struct h5mobaku *ctx, cmph_t *hash, uint32_t mesh_id, const char *datetime_str) {
    if (!ctx || !ctx->h5r_ctx) {
        fprintf(stderr, "Error: Invalid h5mobaku context\n");
        return -1;
    }
    
    int time_index = datetime_to_index(ctx, datetime_str);
    if (time_index < 0) return -1;
    
    return h5mobaku_read_population_single(ctx->h5r_ctx, hash, mesh_id, time_index);
}

int32_t* h5mobaku_read_population_multi_at_time(struct h5mobaku *ctx, cmph_t *hash, uint32_t *mesh_ids, size_t num_meshes, const char *datetime_str) {
    if (!ctx || !ctx->h5r_ctx) {
        fprintf(stderr, "Error: Invalid h5mobaku context\n");
        return NULL;
    }
    
    int time_index = datetime_to_index(ctx, datetime_str);
    if (time_index < 0) return NULL;
    
    return h5mobaku_read_population_multi(ctx->h5r_ctx, hash, mesh_ids, num_meshes, time_index);
}

int32_t* h5mobaku_read_population_time_series_between(struct h5mobaku *ctx, cmph_t *hash, uint32_t mesh_id, const char *start_datetime_str, const char *end_datetime_str) {
    if (!ctx || !ctx->h5r_ctx) {
        fprintf(stderr, "Error: Invalid h5mobaku context\n");
        return NULL;
    }
    
    int start_index = datetime_to_index(ctx, start_datetime_str);
    int end_index = datetime_to_index(ctx, end_datetime_str);
    if (start_index < 0 || end_index < 0) return NULL;
    
    return h5mobaku_read_population_time_series(ctx->h5r_ctx, hash, mesh_id, start_index, end_index);
}



// Read population data for a single mesh at a specific time index
int32_t h5mobaku_read_population_single(struct h5r *h5_ctx, cmph_t *hash, uint32_t mesh_id, int time_index) {
    if (!h5_ctx || !hash || time_index < 0) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_read_population_single\n");
        return -1;
    }
    
    // Get mesh index using CMPH hash
    uint32_t mesh_index = search_id(hash, mesh_id);
    if (mesh_index == MESHID_NOT_FOUND || mesh_index >= 1553332) {  // Check for error or bounds
        return -1;
    }
    
    // Read only the specific cell
    int32_t value;
    int ret = h5r_read_cell(h5_ctx, (uint64_t)time_index, (uint64_t)mesh_index, &value);
    if (ret < 0) {
        fprintf(stderr, "Error: Failed to read cell at time %d, mesh %u from HDF5 file\n", time_index, mesh_id);
        return -1;
    }
    
    return value;
}

// Read population data for multiple meshes at a specific time index
int32_t* h5mobaku_read_population_multi(struct h5r *h5_ctx, cmph_t *hash, uint32_t *mesh_ids, size_t num_meshes, int time_index) {
    if (!h5_ctx || !hash || !mesh_ids || num_meshes == 0 || time_index < 0) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_read_population_multi\n");
        return NULL;
    }
    
    // Allocate result array
    int32_t *results = (int32_t*)malloc(num_meshes * sizeof(int32_t));
    if (!results) {
        fprintf(stderr, "Error: Memory allocation failed for results array\n");
        return NULL;
    }
    
    // Get mesh indices
    uint64_t *mesh_indices = (uint64_t*)malloc(num_meshes * sizeof(uint64_t));
    if (!mesh_indices) {
        fprintf(stderr, "Error: Memory allocation failed for mesh indices\n");
        free(results);
        return NULL;
    }
    
    // Convert mesh IDs to indices
    for (size_t i = 0; i < num_meshes; i++) {
        uint32_t mesh_index = search_id(hash, mesh_ids[i]);
        if (mesh_index == MESHID_NOT_FOUND || mesh_index >= 1553332) {
            fprintf(stderr, "Error: Mesh ID %u not found or invalid\n", mesh_ids[i]);
            free(mesh_indices);
            free(results);
            return NULL;
        }
        mesh_indices[i] = (uint64_t)mesh_index;
    }
    
    // Read only the specific cells
    int ret = h5r_read_cells(h5_ctx, (uint64_t)time_index, mesh_indices, num_meshes, results);
    if (ret < 0) {
        fprintf(stderr, "Error: Failed to read cells at time %d from HDF5 file\n", time_index);
        free(mesh_indices);
        free(results);
        return NULL;
    }
    
    free(mesh_indices);
    return results;
}

// Read population time series for a single mesh
int32_t* h5mobaku_read_population_time_series(struct h5r *h5_ctx, cmph_t *hash, uint32_t mesh_id, int start_time_index, int end_time_index) {
    if (!h5_ctx || !hash || start_time_index < 0 || end_time_index < start_time_index) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_read_population_time_series\n");
        return NULL;
    }
    
    // Get mesh index
    uint32_t mesh_index = search_id(hash, mesh_id);
    if (mesh_index == MESHID_NOT_FOUND || mesh_index >= 1553332) {
        return NULL;
    }
    
    int num_times = end_time_index - start_time_index + 1;
    int32_t *time_series = (int32_t*)malloc(num_times * sizeof(int32_t));
    if (!time_series) {
        fprintf(stderr, "Error: Memory allocation failed for time series data\n");
        return NULL;
    }
    
    // Read each time step using single cell reads
    for (int t = 0; t < num_times; t++) {
        int ret = h5r_read_cell(h5_ctx, (uint64_t)(start_time_index + t), (uint64_t)mesh_index, &time_series[t]);
        if (ret < 0) {
            fprintf(stderr, "Error: Failed to read cell at time %d from HDF5 file\n", start_time_index + t);
            free(time_series);
            return NULL;
        }
    }
    return time_series;
}


// Free allocated memory
void h5mobaku_free_data(int32_t *data) {
    free(data);
}