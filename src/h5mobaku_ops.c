//
// Created by ryuzot on 25/05/31.
//

#define _GNU_SOURCE
#include "h5mobaku_ops.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <hdf5.h>
#include <time.h>

// Helper functions for common operations
static int validate_h5mobaku_context(struct h5mobaku *ctx) {
    if (!ctx) {
        fprintf(stderr, "Error: Invalid h5mobaku context\n");
        return -1;
    }
    if (!ctx->h5r_ctx) {
        fprintf(stderr, "Error: Invalid h5r context in h5mobaku\n");
        return -1;
    }
    return 0;
}

static int validate_basic_params(struct h5r *h5_ctx, cmph_t *hash) {
    if (!h5_ctx) {
        fprintf(stderr, "Error: Invalid h5r context\n");
        return -1;
    }
    if (!hash) {
        fprintf(stderr, "Error: Invalid hash context\n");
        return -1;
    }
    return 0;
}

static uint64_t get_mesh_index(cmph_t *hash, uint32_t mesh_id) {
    uint32_t mesh_index = meshid_search_id(hash, mesh_id);
    if (mesh_index == MESHID_NOT_FOUND || mesh_index >= MOBAKU_MESH_COUNT) {
        return UINT64_MAX; // Error indicator
    }
    return (uint64_t)mesh_index;
}

static void* safe_malloc(size_t size, const char *description) {
    void *ptr = malloc(size);
    if (!ptr) {
        fprintf(stderr, "Error: Memory allocation failed for %s\n", description);
    }
    return ptr;
}

static void cleanup_multi_arrays(uint64_t *mesh_indices, int32_t *results) {
    if (mesh_indices) free(mesh_indices);
    if (results) free(results);
}

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
    if (validate_h5mobaku_context(ctx) < 0) return -1;
    
    int time_index = datetime_to_index(ctx, datetime_str);
    if (time_index < 0) return -1;
    
    return h5mobaku_read_population_single(ctx->h5r_ctx, hash, mesh_id, time_index);
}

int32_t* h5mobaku_read_population_multi_at_time(struct h5mobaku *ctx, cmph_t *hash, uint32_t *mesh_ids, size_t num_meshes, const char *datetime_str) {
    if (validate_h5mobaku_context(ctx) < 0) return NULL;
    
    int time_index = datetime_to_index(ctx, datetime_str);
    if (time_index < 0) return NULL;
    
    return h5mobaku_read_population_multi(ctx->h5r_ctx, hash, mesh_ids, num_meshes, time_index);
}

int32_t* h5mobaku_read_population_time_series_between(struct h5mobaku *ctx, cmph_t *hash, uint32_t mesh_id, const char *start_datetime_str, const char *end_datetime_str) {
    if (validate_h5mobaku_context(ctx) < 0) return NULL;
    
    int start_index = datetime_to_index(ctx, start_datetime_str);
    int end_index = datetime_to_index(ctx, end_datetime_str);
    if (start_index < 0 || end_index < 0) return NULL;
    
    return h5mobaku_read_population_time_series(ctx->h5r_ctx, hash, mesh_id, start_index, end_index);
}



// Read population data for a single mesh at a specific time index
int32_t h5mobaku_read_population_single(struct h5r *h5_ctx, cmph_t *hash, uint32_t mesh_id, int time_index) {
    if (validate_basic_params(h5_ctx, hash) < 0 || time_index < 0) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_read_population_single\n");
        return -1;
    }
    
    uint64_t mesh_index = get_mesh_index(hash, mesh_id);
    if (mesh_index == UINT64_MAX) {
        fprintf(stderr, "Error: Mesh ID %u not found or invalid\n", mesh_id);
        return -1;
    }
    
    int32_t value;
    int ret = h5r_read_cell(h5_ctx, (uint64_t)time_index, mesh_index, &value);
    if (ret < 0) {
        fprintf(stderr, "Error: Failed to read cell at time %d, mesh %u from HDF5 file\n", time_index, mesh_id);
        return -1;
    }
    
    return value;
}

// Read population data for multiple meshes at a specific time index
int32_t* h5mobaku_read_population_multi(struct h5r *h5_ctx, cmph_t *hash, uint32_t *mesh_ids, size_t num_meshes, int time_index) {
    if (validate_basic_params(h5_ctx, hash) < 0 || !mesh_ids || num_meshes == 0 || time_index < 0) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_read_population_multi\n");
        return NULL;
    }
    
    int32_t *results = (int32_t*)safe_malloc(num_meshes * sizeof(int32_t), "results array");
    if (!results) return NULL;
    
    uint64_t *mesh_indices = (uint64_t*)safe_malloc(num_meshes * sizeof(uint64_t), "mesh indices");
    if (!mesh_indices) {
        free(results);
        return NULL;
    }
    
    // Convert mesh IDs to indices
    for (size_t i = 0; i < num_meshes; i++) {
        mesh_indices[i] = get_mesh_index(hash, mesh_ids[i]);
        if (mesh_indices[i] == UINT64_MAX) {
            fprintf(stderr, "Error: Mesh ID %u not found or invalid\n", mesh_ids[i]);
            cleanup_multi_arrays(mesh_indices, results);
            return NULL;
        }
    }
    
    int ret = h5r_read_cells(h5_ctx, (uint64_t)time_index, mesh_indices, num_meshes, results);
    if (ret < 0) {
        fprintf(stderr, "Error: Failed to read cells at time %d from HDF5 file\n", time_index);
        cleanup_multi_arrays(mesh_indices, results);
        return NULL;
    }
    
    free(mesh_indices);
    return results;
}

// Read population time series for a single mesh
int32_t* h5mobaku_read_population_time_series(struct h5r *h5_ctx, cmph_t *hash, uint32_t mesh_id, int start_time_index, int end_time_index) {
    if (validate_basic_params(h5_ctx, hash) < 0 || start_time_index < 0 || end_time_index < start_time_index) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_read_population_time_series\n");
        return NULL;
    }
    
    uint64_t mesh_index = get_mesh_index(hash, mesh_id);
    if (mesh_index == UINT64_MAX) {
        fprintf(stderr, "Error: Mesh ID %u not found or invalid\n", mesh_id);
        return NULL;
    }
    
    int num_times = end_time_index - start_time_index + 1;
    int32_t *time_series = (int32_t*)safe_malloc(num_times * sizeof(int32_t), "time series data");
    if (!time_series) return NULL;
    
    // Use bulk read for better performance with chunking (crows:HDF5_DATETIME_CHUNK;ccols:HDF5_MESH_CHUNK)
    int ret = h5r_read_column_range(h5_ctx, (uint64_t)start_time_index, (uint64_t)end_time_index, mesh_index, time_series);
    if (ret < 0) {
        fprintf(stderr, "Error: Failed to read time series from %d to %d for mesh %u\n", 
                start_time_index, end_time_index, mesh_id);
        free(time_series);
        return NULL;
    }
    
    return time_series;
}

/* ---------------------------------------------------------------- */

int32_t *
h5mobaku_read_multi_mesh_time_series(struct h5r  *h5_ctx,
                                     cmph_t      *hash,
                                     uint32_t    *mesh_ids,
                                     size_t       num_meshes,
                                     int          start_time_index,
                                     int          end_time_index)
{
    TIC(total);

    if (validate_basic_params(h5_ctx, hash) < 0 ||
        !mesh_ids || num_meshes == 0 ||
        start_time_index < 0 || end_time_index < start_time_index)
        return NULL;

    const uint64_t nrows       = (uint64_t)(end_time_index - start_time_index + 1);
    const size_t   total_elems = nrows * num_meshes;

    /* -- 1. Convert mesh_id to dcol -- */
    TIC(map_ids);
    uint64_t *dcols = safe_malloc(num_meshes * sizeof(uint64_t), "dcols");
    if (!dcols) return NULL;
    for (size_t i = 0; i < num_meshes; ++i) {
        dcols[i] = get_mesh_index(hash, mesh_ids[i]);
        if (dcols[i] == UINT64_MAX) { free(dcols); return NULL; }
    }
    TOC(map_ids);

    /* -- 2. Detect contiguous blocks -- */
    TIC(block_detect);
    h5r_block_t *blks = safe_malloc(num_meshes * sizeof(h5r_block_t), "blks");
    if (!blks) { free(dcols); return NULL; }

    size_t nblk = 0;
    for (size_t i = 0; i < num_meshes; ) {
        size_t j = i + 1;
        while (j < num_meshes && dcols[j] == dcols[j-1] + 1) ++j;
        blks[nblk++] = (h5r_block_t){ .dcol0 = dcols[i],
                                      .mcol0 = i,
                                      .ncols = j - i };
        i = j;
    }
    TOC(block_detect);

    /* -- 3. Route branching -- */
    int32_t *buf = NULL;

    if (nblk > NBLK_THRESHOLD) {
        /* ---- UNION hyperslab route ---- */
        TIC(union_total);
        buf = safe_malloc(total_elems * sizeof(int32_t), "result");
        if (!buf) { free(dcols); free(blks); return NULL; }

        TIC(h5_union_read);
        int rv = h5r_read_blocks_union(h5_ctx,
                                       (uint64_t)start_time_index,
                                       nrows,
                                       blks, nblk,
                                       buf,
                                       /* dst_stride */ num_meshes);
        TOC(h5_union_read);

        if (rv < 0) { free(dcols); free(blks); free(buf); return NULL; }
        TOC(union_total);
    }
    else {
        /* ---- Fallback route ---- */
        TIC(fallback_total);
        buf = safe_malloc(total_elems * sizeof(int32_t), "result");
        if (!buf) { free(dcols); free(blks); return NULL; }

        TIC(per_column_reads);
        for (size_t k = 0; k < num_meshes; ++k) {

            TIC(single_read);
            int32_t *col = h5mobaku_read_population_time_series(
                                h5_ctx, hash, mesh_ids[k],
                                start_time_index, end_time_index);
            TOC(single_read);

            if (!col) { free(dcols); free(blks); free(buf); return NULL; }

            /* Row-wise copy (stride = num_meshes) */
            TIC(strided_copy);
            for (uint64_t r = 0; r < nrows; ++r)
                buf[r * num_meshes + k] = col[r];
            TOC(strided_copy);

            free(col);
        }
        TOC(per_column_reads);
        TOC(fallback_total);
    }

    free(dcols);
    free(blks);
    TOC(total);
    return buf;
}


// Free allocated memory
void h5mobaku_free_data(int32_t *data) {
    free(data);
}


/* ===============================================
 *  Writing Functions
 * =============================================== */

int h5mobaku_create(const char *path, const h5r_writer_config_t* config, struct h5mobaku **out) {
    if (!path || !out) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_create\n");
        return -1;
    }
    
    // Use default config if none provided
    h5r_writer_config_t actual_config;
    if (config) {
        actual_config = *config;
    } else {
        h5r_writer_config_t default_config = H5R_WRITER_DEFAULT_CONFIG;
        actual_config = default_config;
    }
    
    // Create file using HDF5 API directly
    hid_t fcpl = H5Pcreate(H5P_FILE_CREATE);
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    
    // Set chunk cache size
    size_t cache_bytes = actual_config.cache_size_mb * 1024 * 1024;
    H5Pset_cache(fapl, 0, 521, cache_bytes, 0.75);
    
    hid_t file = H5Fcreate(path, H5F_ACC_TRUNC, fcpl, fapl);
    H5Pclose(fcpl);
    H5Pclose(fapl);
    
    if (file < 0) {
        fprintf(stderr, "Error: Failed to create HDF5 file\n");
        return -1;
    }
    
    // Create dataspace
    hsize_t dims[2] = {actual_config.initial_time_points, MOBAKU_MESH_COUNT};
    hsize_t maxdims[2] = {H5S_UNLIMITED, MOBAKU_MESH_COUNT};
    hid_t dataspace_id = H5Screate_simple(2, dims, maxdims);
    
    if (dataspace_id < 0) {
        H5Fclose(file);
        return -1;
    }
    
    // Create dataset creation property list
    hid_t dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    
    // Set chunk size
    hsize_t chunk_dims[2] = {actual_config.chunk_time_size, actual_config.chunk_mesh_size};
    H5Pset_chunk(dcpl_id, 2, chunk_dims);
    
    // Set compression if requested
    if (actual_config.compression_level > 0) {
        H5Pset_deflate(dcpl_id, actual_config.compression_level);
    }
    
    // Set fill value to 0
    int32_t fill_value = 0;
    H5Pset_fill_value(dcpl_id, H5T_NATIVE_INT, &fill_value);
    
    // Create dataset
    hid_t dset = H5Dcreate2(file, "population_data", 
                           H5T_NATIVE_INT, dataspace_id,
                           H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    if (dset < 0) {
        H5Pclose(dcpl_id);
        H5Sclose(dataspace_id);
        H5Fclose(file);
        return -1;
    }
    
    // Create meshid_list metadata dataset
    hsize_t meshid_list_dims[1] = {MOBAKU_MESH_COUNT};
    hid_t meshid_list_space_id = H5Screate_simple(1, meshid_list_dims, NULL);
    if (meshid_list_space_id >= 0) {
        hid_t meshid_list_dataset_id = H5Dcreate(file, "meshid_list", H5T_NATIVE_UINT32, 
                                                meshid_list_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (meshid_list_dataset_id >= 0) {
            // Write the mesh ID list from meshid_ops.h extern array
            H5Dwrite(meshid_list_dataset_id, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, meshid_list);
            H5Dclose(meshid_list_dataset_id);
        }
        H5Sclose(meshid_list_space_id);
    }
    
    // Create cmph_data dataset
    size_t mph_size = (size_t)(_binary_meshid_mobaku_mph_end - _binary_meshid_mobaku_mph_start);
    hsize_t cmph_data_dims[1] = {mph_size};
    hid_t cmph_data_space_id = H5Screate_simple(1, cmph_data_dims, NULL);
    if (cmph_data_space_id >= 0) {
        hid_t cmph_data_dataset_id = H5Dcreate(file, "cmph_data", H5T_NATIVE_UINT8, 
                                              cmph_data_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (cmph_data_dataset_id >= 0) {
            // Write the CMPH data from meshid_ops.h extern array
            H5Dwrite(cmph_data_dataset_id, H5T_NATIVE_UINT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, _binary_meshid_mobaku_mph_start);
            H5Dclose(cmph_data_dataset_id);
        }
        H5Sclose(cmph_data_space_id);
    }
    
    // Close HDF5 resources
    H5Dclose(dset);
    H5Pclose(dcpl_id);
    H5Sclose(dataspace_id);
    H5Fclose(file);
    
    // Now open the file for read/write using h5mobaku_open_readwrite
    return h5mobaku_open_readwrite(path, out);
}

int h5mobaku_open_readwrite(const char *path, struct h5mobaku **out) {
    if (!path || !out) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_open_readwrite\n");
        return -1;
    }
    
    struct h5mobaku *ctx = calloc(1, sizeof(struct h5mobaku));
    if (!ctx) {
        fprintf(stderr, "Error: Memory allocation failed for h5mobaku context\n");
        return -1;
    }
    
    // Open the underlying h5r context for read/write
    int ret = h5r_open_readwrite(path, &ctx->h5r_ctx);
    if (ret < 0) {
        free(ctx);
        return -1;
    }
    
    // Try to read start_datetime attribute, use default if not found
    ctx->start_datetime = REFERENCE_MOBAKU_TIME;
    ctx->start_datetime_str = strdup(REFERENCE_MOBAKU_DATETIME);
    
    *out = ctx;
    return 0;
}

int h5mobaku_write_population_single_at_time(struct h5mobaku *ctx, cmph_t *hash, uint32_t mesh_id, const char *datetime_str, int32_t value) {
    if (validate_h5mobaku_context(ctx) < 0) return -1;
    
    int time_index = datetime_to_index(ctx, datetime_str);
    if (time_index < 0) return -1;
    
    return h5mobaku_write_population_single(ctx->h5r_ctx, hash, mesh_id, time_index, value);
}

int h5mobaku_write_population_multi_at_time(struct h5mobaku *ctx, cmph_t *hash, uint32_t *mesh_ids, const int32_t *values, size_t num_meshes, const char *datetime_str) {
    if (validate_h5mobaku_context(ctx) < 0) return -1;
    
    int time_index = datetime_to_index(ctx, datetime_str);
    if (time_index < 0) return -1;
    
    return h5mobaku_write_population_multi(ctx->h5r_ctx, hash, mesh_ids, values, num_meshes, time_index);
}

int h5mobaku_write_population_single(struct h5r *h5_ctx, cmph_t *hash, uint32_t mesh_id, int time_index, int32_t value) {
    if (validate_basic_params(h5_ctx, hash) < 0 || time_index < 0) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_write_population_single\n");
        return -1;
    }
    
    uint64_t mesh_index = get_mesh_index(hash, mesh_id);
    if (mesh_index == UINT64_MAX) {
        fprintf(stderr, "Error: Mesh ID %u not found or invalid\n", mesh_id);
        return -1;
    }
    
    int ret = h5r_write_cell(h5_ctx, (uint64_t)time_index, mesh_index, value);
    if (ret < 0) {
        fprintf(stderr, "Error: Failed to write cell at time %d, mesh %u to HDF5 file\n", time_index, mesh_id);
        return -1;
    }
    
    return 0;
}

int h5mobaku_write_population_multi(struct h5r *h5_ctx, cmph_t *hash, uint32_t *mesh_ids, const int32_t *values, size_t num_meshes, int time_index) {
    if (validate_basic_params(h5_ctx, hash) < 0 || !mesh_ids || !values || num_meshes == 0 || time_index < 0) {
        fprintf(stderr, "Error: Invalid parameters in h5mobaku_write_population_multi\n");
        return -1;
    }
    
    uint64_t *mesh_indices = (uint64_t*)safe_malloc(num_meshes * sizeof(uint64_t), "mesh indices");
    if (!mesh_indices) return -1;
    
    // Convert mesh IDs to indices
    for (size_t i = 0; i < num_meshes; i++) {
        mesh_indices[i] = get_mesh_index(hash, mesh_ids[i]);
        if (mesh_indices[i] == UINT64_MAX) {
            fprintf(stderr, "Error: Mesh ID %u not found or invalid\n", mesh_ids[i]);
            free(mesh_indices);
            return -1;
        }
    }
    
    int ret = h5r_write_cells(h5_ctx, (uint64_t)time_index, mesh_indices, values, num_meshes);
    if (ret < 0) {
        fprintf(stderr, "Error: Failed to write cells at time %d to HDF5 file\n", time_index);
        free(mesh_indices);
        return -1;
    }
    
    free(mesh_indices);
    return 0;
}

int h5mobaku_extend_time_dimension(struct h5mobaku *ctx, size_t new_time_points) {
    if (validate_h5mobaku_context(ctx) < 0) return -1;
    
    return h5r_extend_time_dimension(ctx->h5r_ctx, new_time_points);
}

int h5mobaku_flush(struct h5mobaku *ctx) {
    if (validate_h5mobaku_context(ctx) < 0) return -1;
    
    return h5r_flush(ctx->h5r_ctx);
}