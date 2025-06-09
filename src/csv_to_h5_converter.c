//
// CSV to HDF5 converter implementation
//

#include "csv_to_h5_converter.h"
#include "csv_ops.h"
#include "h5mobaku_ops.h"
#include "meshid_ops.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <fnmatch.h>
#include <sys/stat.h>

// Internal structure to track unique timestamps and their indices
typedef struct {
    uint32_t date;
    uint16_t time;
    size_t index;
} timestamp_entry_t;

// Internal converter context
typedef struct {
    struct h5mobaku* writer;
    cmph_t* mesh_hash;
    timestamp_entry_t* timestamps;
    size_t timestamp_count;
    size_t timestamp_capacity;
    int32_t* batch_buffer;
    size_t batch_size;
    csv_to_h5_stats_t stats;
} converter_ctx_t;

static int timestamp_compare(const void* a, const void* b) {
    const timestamp_entry_t* ta = (const timestamp_entry_t*)a;
    const timestamp_entry_t* tb = (const timestamp_entry_t*)b;
    
    if (ta->date != tb->date) return (ta->date > tb->date) - (ta->date < tb->date);
    return (ta->time > tb->time) - (ta->time < tb->time);
}

static size_t find_or_add_timestamp(converter_ctx_t* ctx, uint32_t date, uint16_t time) {
    // Binary search for existing timestamp
    timestamp_entry_t key = {date, time, 0};
    timestamp_entry_t* found = bsearch(&key, ctx->timestamps, ctx->timestamp_count,
                                      sizeof(timestamp_entry_t), timestamp_compare);
    
    if (found) {
        return found->index;
    }
    
    // Add new timestamp
    if (ctx->timestamp_count >= ctx->timestamp_capacity) {
        size_t new_capacity = ctx->timestamp_capacity * 2;
        timestamp_entry_t* new_timestamps = realloc(ctx->timestamps, 
                                                   new_capacity * sizeof(timestamp_entry_t));
        if (!new_timestamps) return (size_t)-1;
        
        ctx->timestamps = new_timestamps;
        ctx->timestamp_capacity = new_capacity;
    }
    
    ctx->timestamps[ctx->timestamp_count].date = date;
    ctx->timestamps[ctx->timestamp_count].time = time;
    ctx->timestamps[ctx->timestamp_count].index = ctx->timestamp_count;
    ctx->timestamp_count++;
    
    // Re-sort array
    qsort(ctx->timestamps, ctx->timestamp_count, sizeof(timestamp_entry_t), timestamp_compare);
    
    // Find again after sorting
    found = bsearch(&key, ctx->timestamps, ctx->timestamp_count,
                   sizeof(timestamp_entry_t), timestamp_compare);
    
    return found ? found->index : (size_t)-1;
}

static converter_ctx_t* converter_create(const csv_to_h5_config_t* config) {
    converter_ctx_t* ctx = calloc(1, sizeof(converter_ctx_t));
    if (!ctx) return NULL;
    
    // Initialize mesh hash
    ctx->mesh_hash = meshid_prepare_search();
    if (!ctx->mesh_hash) {
        free(ctx);
        return NULL;
    }
    
    // Initialize timestamp tracking
    ctx->timestamp_capacity = 1000;
    ctx->timestamps = malloc(ctx->timestamp_capacity * sizeof(timestamp_entry_t));
    if (!ctx->timestamps) {
        cmph_destroy(ctx->mesh_hash);
        free(ctx);
        return NULL;
    }
    
    // Initialize batch buffer
    ctx->batch_size = config->batch_size;
    ctx->batch_buffer = calloc(MOBAKU_MESH_COUNT, sizeof(int32_t));
    if (!ctx->batch_buffer) {
        free(ctx->timestamps);
        cmph_destroy(ctx->mesh_hash);
        free(ctx);
        return NULL;
    }
    
    // Create or open HDF5 file
    if (config->create_new) {
        h5r_writer_config_t h5_config = H5R_WRITER_DEFAULT_CONFIG;
        ctx->writer = NULL;
        if (h5mobaku_create(config->output_h5_file, &h5_config, &ctx->writer) < 0) {
            ctx->writer = NULL;
        }
    } else {
        ctx->writer = NULL;
        if (h5mobaku_open_readwrite(config->output_h5_file, &ctx->writer) < 0) {
            ctx->writer = NULL;
        }
    }
    
    if (!ctx->writer) {
        free(ctx->batch_buffer);
        free(ctx->timestamps);
        cmph_destroy(ctx->mesh_hash);
        free(ctx);
        return NULL;
    }
    
    return ctx;
}

static void converter_destroy(converter_ctx_t* ctx) {
    if (!ctx) return;
    
    if (ctx->writer) h5mobaku_close(ctx->writer);
    if (ctx->mesh_hash) cmph_destroy(ctx->mesh_hash);
    if (ctx->timestamps) free(ctx->timestamps);
    if (ctx->batch_buffer) free(ctx->batch_buffer);
    free(ctx);
}

static int process_csv_row(converter_ctx_t* ctx, const csv_row_t* row, int verbose) {
    // Find time index
    size_t time_idx = find_or_add_timestamp(ctx, row->date, row->time);
    if (time_idx == (size_t)-1) {
        ctx->stats.errors++;
        return -1;
    }
    
    // Extend HDF5 file if needed
    size_t current_time_points, mesh_count;
    h5r_get_dimensions(ctx->writer->h5r_ctx, &current_time_points, &mesh_count);
    
    if (time_idx >= current_time_points) {
        // Extend by 50% or at least to accommodate new index
        size_t new_size = current_time_points * 3 / 2;
        if (new_size <= time_idx) new_size = time_idx + 100;
        
        if (h5mobaku_extend_time_dimension(ctx->writer, new_size) < 0) {
            ctx->stats.errors++;
            return -1;
        }
    }
    
    // Find mesh index
    uint32_t mesh_idx = meshid_search_id(ctx->mesh_hash, (uint32_t)row->area);
    if (mesh_idx == MESHID_NOT_FOUND) {
        if (verbose) {
            fprintf(stderr, "Warning: Unknown mesh ID %lu\n", row->area);
        }
        ctx->stats.errors++;
        return -1;
    }
    
    // Write value
    if (h5mobaku_write_population_single(ctx->writer->h5r_ctx, ctx->mesh_hash, (uint32_t)row->area, (int)time_idx, row->population) < 0) {
        ctx->stats.errors++;
        return -1;
    }
    
    ctx->stats.total_rows_processed++;
    return 0;
}

int csv_to_h5_convert_file(const char* csv_filename, const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats) {
    const char* files[] = {csv_filename};
    return csv_to_h5_convert_files(files, 1, config, stats);
}

static int convert_single_file(converter_ctx_t* ctx, const char* csv_filename, const csv_to_h5_config_t* config) {
    // Open CSV file
    csv_reader_t* reader = csv_open(csv_filename);
    if (!reader) {
        return -1;
    }
    
    if (config->verbose) {
        printf("Processing %s...\n", csv_filename);
    }
    
    // Process CSV rows
    csv_row_t row;
    int result;
    size_t row_count = 0;
    
    while ((result = csv_read_row(reader, &row)) == 0) {
        if (process_csv_row(ctx, &row, config->verbose) < 0) {
            if (config->verbose) {
                fprintf(stderr, "Error processing row %zu in %s\n", 
                       csv_get_line_number(reader), csv_filename);
            }
        }
        
        row_count++;
        if (config->verbose && row_count % 10000 == 0) {
            printf("  Processed %zu rows...\n", row_count);
        }
    }
    
    if (result < 0) {
        fprintf(stderr, "Error reading CSV file at line %zu\n", csv_get_line_number(reader));
        csv_close(reader);
        return -1;
    }
    
    if (config->verbose) {
        printf("Completed processing %s: %zu rows\n", csv_filename, row_count);
    }
    
    csv_close(reader);
    return 0;
}

int csv_to_h5_convert_files(const char** csv_filenames, size_t num_files, 
                           const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats) {
    if (!csv_filenames || num_files == 0) return -1;
    
    csv_to_h5_config_t local_config = config ? *config : (csv_to_h5_config_t)CSV_TO_H5_DEFAULT_CONFIG;
    
    // Create converter context (handles both create and append modes)
    converter_ctx_t* ctx = converter_create(&local_config);
    if (!ctx) return -1;
    
    // Process all files
    for (size_t i = 0; i < num_files; i++) {
        if (convert_single_file(ctx, csv_filenames[i], &local_config) < 0) {
            if (stats) {
                stats->total_rows_processed = ctx->stats.total_rows_processed;
                stats->unique_timestamps = ctx->timestamp_count;
                stats->unique_meshes = MOBAKU_MESH_COUNT;
                stats->errors = ctx->stats.errors;
            }
            converter_destroy(ctx);
            return -1;
        }
    }
    
    // Flush data
    h5mobaku_flush(ctx->writer);
    
    // Final statistics
    if (stats) {
        stats->total_rows_processed = ctx->stats.total_rows_processed;
        stats->unique_timestamps = ctx->timestamp_count;
        stats->unique_meshes = MOBAKU_MESH_COUNT;
        stats->errors = ctx->stats.errors;
    }
    
    converter_destroy(ctx);
    return 0;
}

int csv_to_h5_convert_directory(const char* directory, const char* pattern,
                               const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats) {
    if (!directory || !pattern) return -1;
    
    struct dirent* entry;
    DIR* dir = opendir(directory);
    if (!dir) return -1;
    
    // Collect matching files
    char** filenames = NULL;
    size_t file_count = 0;
    size_t file_capacity = 10;
    
    filenames = malloc(file_capacity * sizeof(char*));
    if (!filenames) {
        closedir(dir);
        return -1;
    }
    
    while ((entry = readdir(dir)) != NULL) {
        if (fnmatch(pattern, entry->d_name, 0) == 0) {
            if (file_count >= file_capacity) {
                file_capacity *= 2;
                char** new_filenames = realloc(filenames, file_capacity * sizeof(char*));
                if (!new_filenames) {
                    for (size_t i = 0; i < file_count; i++) free(filenames[i]);
                    free(filenames);
                    closedir(dir);
                    return -1;
                }
                filenames = new_filenames;
            }
            
            // Build full path
            size_t path_len = strlen(directory) + strlen(entry->d_name) + 2;
            filenames[file_count] = malloc(path_len);
            if (!filenames[file_count]) {
                for (size_t i = 0; i < file_count; i++) free(filenames[i]);
                free(filenames);
                closedir(dir);
                return -1;
            }
            
            snprintf(filenames[file_count], path_len, "%s/%s", directory, entry->d_name);
            file_count++;
        }
    }
    closedir(dir);
    
    if (file_count == 0) {
        free(filenames);
        return -1;
    }
    
    // Sort filenames for consistent ordering
    qsort(filenames, file_count, sizeof(char*), (int(*)(const void*, const void*))strcmp);
    
    // Convert files
    int result = csv_to_h5_convert_files((const char**)filenames, file_count, config, stats);
    
    // Cleanup
    for (size_t i = 0; i < file_count; i++) free(filenames[i]);
    free(filenames);
    
    return result;
}