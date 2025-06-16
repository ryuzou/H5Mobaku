//
// CSV to HDF5 converter implementation
//

#include "csv_to_h5_converter.h"
#include "csv_ops.h"
#include "h5mobaku_ops.h"
#include "meshid_ops.h"
#include "fifioq.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <fnmatch.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sys/mman.h>
#include <stdbool.h>
#include <time.h>
#include <hdf5.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <unistd.h>

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
    pthread_mutex_t stats_mutex;
    pthread_mutex_t timestamp_mutex;
    // Bulk write buffer for year-wise processing (51 GiB)
    int32_t* year_buffer;
    size_t year_buffer_size;
    bool use_bulk_write;
    int bulk_write_year;  // Year of data for bulk mode
} converter_ctx_t;

// Pre-processed write data structure (sent from producer to consumer)
typedef struct {
    size_t time_index;    // Calculated time index for HDF5
    uint32_t mesh_index;  // Calculated mesh index  
    int32_t population;   // Population value to write
    bool use_bulk_mode;   // Whether to use bulk buffer or direct write
} write_data_t;

// Consumer thread data for parallel processing
typedef struct {
    converter_ctx_t* ctx;
    FIFOQueue* queue;
    const csv_to_h5_config_t* config;
    volatile int* should_stop;
} consumer_thread_data_t;

// Enhanced CSV reader thread data structure that includes converter context
typedef struct {
    int thread_id;
    char** filepaths;
    size_t num_files;
    FIFOQueue* queue;
    size_t* rows_processed;
    pthread_mutex_t* stats_mutex;
    converter_ctx_t* ctx;  // Added for processing
    bool verbose;
    size_t* total_files_processed;  // For progress tracking
    size_t total_files;             // Total number of files
} enhanced_csv_reader_thread_data_t;

// Progress bar display function
static void display_progress(size_t current, size_t total, const char* prefix) {
    if (!isatty(STDOUT_FILENO)) {
        // Not a terminal, just print percentage
        if (current % 100 == 0 || current == total) {
            printf("%s: %zu/%zu (%.1f%%)\n", prefix, current, total, 
                   100.0 * current / total);
        }
        return;
    }
    
    // Get terminal width
    struct winsize w;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
    int term_width = w.ws_col > 0 ? w.ws_col : 80;
    
    // Calculate progress bar width (leave space for text)
    int bar_width = term_width - 40;
    if (bar_width < 20) bar_width = 20;
    if (bar_width > 100) bar_width = 100;
    
    float progress = (float)current / total;
    int filled = (int)(progress * bar_width);
    
    // Print progress bar
    printf("\r%s: [", prefix);
    for (int i = 0; i < bar_width; i++) {
        if (i < filled) {
            printf("=");
        } else if (i == filled) {
            printf(">");
        } else {
            printf(" ");
        }
    }
    printf("] %.1f%% (%zu/%zu)", progress * 100, current, total);
    
    // Clear to end of line
    printf("\033[K");
    
    if (current == total) {
        printf("\n");
    }
    fflush(stdout);
}

static int timestamp_compare(const void* a, const void* b) {
    const timestamp_entry_t* ta = (const timestamp_entry_t*)a;
    const timestamp_entry_t* tb = (const timestamp_entry_t*)b;
    
    if (ta->date != tb->date) return (ta->date > tb->date) - (ta->date < tb->date);
    return (ta->time > tb->time) - (ta->time < tb->time);
}

static size_t find_or_add_timestamp(converter_ctx_t* ctx, uint32_t date, uint16_t time) {
    pthread_mutex_lock(&ctx->timestamp_mutex);
    
    // Binary search for existing timestamp
    timestamp_entry_t key = {date, time, 0};
    timestamp_entry_t* found = bsearch(&key, ctx->timestamps, ctx->timestamp_count,
                                      sizeof(timestamp_entry_t), timestamp_compare);
    
    if (found) {
        size_t index = found->index;
        pthread_mutex_unlock(&ctx->timestamp_mutex);
        return index;
    }
    
    // Add new timestamp
    if (ctx->timestamp_count >= ctx->timestamp_capacity) {
        size_t new_capacity = ctx->timestamp_capacity * 2;
        timestamp_entry_t* new_timestamps = realloc(ctx->timestamps, 
                                                   new_capacity * sizeof(timestamp_entry_t));
        if (!new_timestamps) {
            pthread_mutex_unlock(&ctx->timestamp_mutex);
            return (size_t)-1;
        }
        
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
    
    size_t result = found ? found->index : (size_t)-1;
    pthread_mutex_unlock(&ctx->timestamp_mutex);
    return result;
}

static int allocate_year_buffer(converter_ctx_t* ctx, bool verbose) {
    // Calculate buffer size: 8760 hours × MOBAKU_MESH_COUNT meshes × 4 bytes ≈ 51 GiB
    const size_t HOURS_PER_YEAR = 8760; // Standard year: 365 days * 24 hours
    const size_t MESH_COUNT = MOBAKU_MESH_COUNT;
    const size_t BYTES_PER_ELEMENT = sizeof(int32_t);
    
    ctx->year_buffer_size = HOURS_PER_YEAR * MESH_COUNT * BYTES_PER_ELEMENT;
    
    if (verbose) {
        printf("Attempting to allocate %.2f GiB for year buffer...\n", 
               (double)ctx->year_buffer_size / (1024.0 * 1024.0 * 1024.0));
    }
    
    // Try aligned allocation with HugeTLB optimization
    void* buf_raw = NULL;
    int result = posix_memalign(&buf_raw, 4096, ctx->year_buffer_size);
    
    if (result != 0) {
        if (verbose) {
            fprintf(stderr, "Failed to allocate aligned memory: %s\n", strerror(result));
        }
        // Fallback to regular allocation
        buf_raw = malloc(ctx->year_buffer_size);
        if (!buf_raw) {
            if (verbose) {
                fprintf(stderr, "Failed to allocate regular memory for year buffer\n");
            }
            return -1;
        }
    } else {
        // Enable HugeTLB for better performance
        if (madvise(buf_raw, ctx->year_buffer_size, MADV_HUGEPAGE) != 0) {
            if (verbose) {
                fprintf(stderr, "Warning: Could not enable HugeTLB optimization\n");
            }
        }
    }
    
    ctx->year_buffer = (int32_t*)buf_raw;
    
    // Initialize buffer to zero
    memset(ctx->year_buffer, 0, ctx->year_buffer_size);
    
    if (verbose) {
        printf("Successfully allocated year buffer (%.2f GiB)\n", 
               (double)ctx->year_buffer_size / (1024.0 * 1024.0 * 1024.0));
    }
    
    return 0;
}

static converter_ctx_t* converter_create(const csv_to_h5_config_t* config) {
    converter_ctx_t* ctx = calloc(1, sizeof(converter_ctx_t));
    if (!ctx) return NULL;
    
    // Initialize mutexes
    if (pthread_mutex_init(&ctx->stats_mutex, NULL) != 0) {
        free(ctx);
        return NULL;
    }
    if (pthread_mutex_init(&ctx->timestamp_mutex, NULL) != 0) {
        pthread_mutex_destroy(&ctx->stats_mutex);
        free(ctx);
        return NULL;
    }
    
    // Initialize mesh hash
    ctx->mesh_hash = meshid_prepare_search();
    if (!ctx->mesh_hash) {
        pthread_mutex_destroy(&ctx->timestamp_mutex);
        pthread_mutex_destroy(&ctx->stats_mutex);
        free(ctx);
        return NULL;
    }
    
    // Initialize timestamp tracking
    ctx->timestamp_capacity = 10000; // Increased for year-wise processing
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
    
    // Check if bulk write mode is enabled
    ctx->use_bulk_write = config->use_bulk_write;
    ctx->year_buffer = NULL;
    ctx->year_buffer_size = 0;
    
    // Allocate year buffer if bulk write is enabled
    if (ctx->use_bulk_write) {
        if (allocate_year_buffer(ctx, config->verbose) < 0) {
            if (config->verbose) {
                fprintf(stderr, "Falling back to incremental write mode\n");
            }
            ctx->use_bulk_write = false;
        }
    }
    
    // Create or open HDF5 file
    if (config->create_new) {
        h5r_writer_config_t h5_config = H5R_WRITER_DEFAULT_CONFIG;
        ctx->writer = NULL;
        // Use configurable dataset name, fallback to default if not specified
        const char* dataset_name = config->dataset_name ? config->dataset_name : "/population_data";
        if (h5mobaku_create_with_dataset(config->output_h5_file, dataset_name, &h5_config, &ctx->writer) < 0) {
            ctx->writer = NULL;
        }
    } else {
        ctx->writer = NULL;
        // Use configurable dataset name, fallback to default if not specified
        const char* dataset_name = config->dataset_name ? config->dataset_name : "/population_data";
        if (h5mobaku_open_readwrite_with_dataset(config->output_h5_file, dataset_name, &ctx->writer) < 0) {
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
    if (ctx->year_buffer) free(ctx->year_buffer);
    pthread_mutex_destroy(&ctx->timestamp_mutex);
    pthread_mutex_destroy(&ctx->stats_mutex);
    free(ctx);
}


static int perform_bulk_write(converter_ctx_t* ctx, int verbose) {
    if (!ctx->use_bulk_write || !ctx->year_buffer) {
        return 0; // Nothing to do if not in bulk write mode
    }
    
    if (verbose) {
        printf("Performing bulk HDF5 write (%.2f GiB)...\n", 
               (double)ctx->year_buffer_size / (1024.0 * 1024.0 * 1024.0));
    }
    
    // Calculate the start time index based on the year of data
    // Bulk mode assumes a single year of data, so we need to determine which year
    // For now, we'll need to pass this information or detect it from the data
    // TODO: This needs to be properly tracked during data loading
    int data_year = ctx->bulk_write_year; // Need to add this field to track the year
    
    // Calculate hours since 2016-01-01 00:00:00 for the start of data_year
    struct tm base_tm = {0};
    base_tm.tm_year = 2016 - 1900;
    base_tm.tm_mon = 0;
    base_tm.tm_mday = 1;
    
    struct tm year_tm = {0};
    year_tm.tm_year = data_year - 1900;
    year_tm.tm_mon = 0;
    year_tm.tm_mday = 1;
    
    time_t base_time = mktime(&base_tm);
    time_t year_time = mktime(&year_tm);
    size_t start_time_idx = (year_time - base_time) / 3600;
    
    if (verbose) {
        printf("Bulk write year: %d, start time index: %zu\n", data_year, start_time_idx);
    }
    
    // First ensure HDF5 dataset has correct dimensions
    size_t current_time_points, mesh_count;
    h5r_get_dimensions(ctx->writer->h5r_ctx, &current_time_points, &mesh_count);
    
    const size_t REQUIRED_TIME_POINTS = HDF5_DATETIME_CHUNK; // Standard year: 365 days * 24 hours
    const size_t REQUIRED_MESH_COUNT = MOBAKU_MESH_COUNT;
    
    // Ensure dataset is large enough for the target year
    size_t needed_time_points = start_time_idx + REQUIRED_TIME_POINTS;
    if (current_time_points < needed_time_points) {
        if (verbose) {
            printf("Extending HDF5 dataset to %zu time points...\n", needed_time_points);
        }
        if (h5mobaku_extend_time_dimension(ctx->writer, needed_time_points) < 0) {
            fprintf(stderr, "Error: Failed to extend HDF5 dataset for bulk write\n");
            return -1;
        }
    }
    
    // True bulk write: write entire buffer in single HDF5 operation
    if (verbose) {
        printf("Performing true bulk write of entire buffer (%.2f GiB)...\n", 
               (double)ctx->year_buffer_size / (1024.0 * 1024.0 * 1024.0));
        display_progress(0, 1, "HDF5 Bulk Write");
    }
    
    // Perform single bulk write operation at the correct time index
    if (h5r_write_bulk_buffer(ctx->writer->h5r_ctx, ctx->year_buffer, 
                              REQUIRED_TIME_POINTS, REQUIRED_MESH_COUNT, start_time_idx) < 0) {
        fprintf(stderr, "Error: Bulk buffer write failed\n");
        return -1;
    }
    
    if (verbose) {
        display_progress(1, 1, "HDF5 Bulk Write");
        printf("Bulk HDF5 write completed successfully\n");
    }
    
    // Flush to ensure data is written
    if (h5mobaku_flush(ctx->writer) < 0) {
        fprintf(stderr, "Warning: Failed to flush after bulk write\n");
    }
    
    return 0;
}

// Consumer thread function that processes pre-calculated write data from the queue
static void* h5_consumer_thread_func(void* arg) {
    consumer_thread_data_t* data = (consumer_thread_data_t*)arg;
    
    if (data->config->verbose) {
        printf("H5 consumer thread started\n");
    }
    
    // If bulk mode is enabled, consumer does nothing - producers write directly to buffer
    if (data->ctx->use_bulk_write && data->ctx->year_buffer) {
        if (data->config->verbose) {
            printf("H5 consumer: Bulk mode enabled, consumer idle (producers write directly to buffer)\n");
        }
        
        // Wait for shutdown signal
        while (!(*data->should_stop)) {
            // Check for sentinel value indicating shutdown
            write_data_t* write_data = (write_data_t*)dequeue(data->queue);
            if (!write_data) {
                if (data->config->verbose) {
                    printf("H5 consumer: Received shutdown signal, stopping\n");
                }
                break;
            }
            // In bulk mode, we shouldn't receive any data, but if we do, just free it
            free(write_data);
        }
    } else {
        // Incremental mode: process write operations from queue
        while (!(*data->should_stop)) {
            // Dequeue will block until data is available
            write_data_t* write_data = (write_data_t*)dequeue(data->queue);
            
            // Check if this is a sentinel value (NULL) indicating shutdown
            if (!write_data) {
                if (data->config->verbose) {
                    printf("H5 consumer: Received shutdown signal, stopping\n");
                }
                break;
            }
            
            // Incremental mode: write directly to HDF5
            // Extend HDF5 file if needed
            size_t current_time_points, mesh_count;
            h5r_get_dimensions(data->ctx->writer->h5r_ctx, &current_time_points, &mesh_count);
            
            if (write_data->time_index >= current_time_points) {
                size_t new_size = current_time_points * 3 / 2;
                if (new_size <= write_data->time_index) new_size = write_data->time_index + 100;
                
                if (h5mobaku_extend_time_dimension(data->ctx->writer, new_size) < 0) {
                    pthread_mutex_lock(&data->ctx->stats_mutex);
                    data->ctx->stats.errors++;
                    pthread_mutex_unlock(&data->ctx->stats_mutex);
                    free(write_data);
                    continue;
                }
            }
            
            
            // Write single cell to HDF5
            if (h5r_write_cell(data->ctx->writer->h5r_ctx, write_data->time_index, write_data->mesh_index, write_data->population) < 0) {
                pthread_mutex_lock(&data->ctx->stats_mutex);
                data->ctx->stats.errors++;
                pthread_mutex_unlock(&data->ctx->stats_mutex);
            }
            
            // Update statistics
            pthread_mutex_lock(&data->ctx->stats_mutex);
            data->ctx->stats.total_rows_processed++;
            pthread_mutex_unlock(&data->ctx->stats_mutex);
            
            // Clean up
            free(write_data);
        }
    }
    
    if (data->config->verbose) {
        printf("H5 consumer thread finished\n");
    }
    return NULL;
}

// Enhanced CSV reader thread function that does preprocessing on producer side
static void* enhanced_csv_reader_thread_func(void* arg) {
    enhanced_csv_reader_thread_data_t* data = (enhanced_csv_reader_thread_data_t*)arg;
    
    if (data->verbose) {
        printf("Enhanced CSV reader thread %d started, processing %zu files\n", 
               data->thread_id, data->num_files);
    }
    
    for (size_t i = 0; i < data->num_files; i++) {
        const char* filepath = data->filepaths[i];
        csv_reader_t* reader = csv_open(filepath);
        
        if (!reader) {
            fprintf(stderr, "Thread %d: Failed to open %s\n", 
                    data->thread_id, filepath);
            continue;
        }
        
        csv_row_t row;
        size_t row_count = 0;
        
        while (csv_read_row(reader, &row) == 0) {
            // Process the CSV row to calculate indices
            write_data_t* write_data = malloc(sizeof(write_data_t));
            if (!write_data) {
                fprintf(stderr, "Thread %d: Failed to allocate memory\n", 
                        data->thread_id);
                break;
            }
            
            // Find mesh index
            uint32_t mesh_idx = meshid_search_id(data->ctx->mesh_hash, (uint32_t)row.area);
            if (mesh_idx == MESHID_NOT_FOUND) {
                if (data->verbose) {
                    fprintf(stderr, "Thread %d: Unknown mesh ID %lu\n", 
                           data->thread_id, row.area);
                }
                free(write_data);
                continue;
            }
            
            // Calculate time index
            size_t time_idx;
            if (data->ctx->use_bulk_write && data->ctx->year_buffer) {
                // Bulk mode: calculate year-relative hour index
                int year = row.date / 10000;
                int month = (row.date / 100) % 100;
                int day = row.date % 100;
                int hour = row.time / 100;
                
                // Track the year for bulk write (assume all data is from same year)
                if (data->ctx->bulk_write_year == 0) {
                    data->ctx->bulk_write_year = year;
                }
                
                // Calculate day of year (0-based)
                int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
                bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
                if (is_leap) days_in_month[1] = 29;
                
                int day_of_year = day - 1;
                for (int m = 1; m < month; m++) {
                    day_of_year += days_in_month[m - 1];
                }
                
                time_idx = day_of_year * 24 + hour;
                size_t max_hours = is_leap ? 8784 : 8760;
                
                if (time_idx >= max_hours) {
                    if (data->verbose) {
                        fprintf(stderr, "Thread %d: Hour index %zu out of range for date %u time %u\n", 
                               data->thread_id, time_idx, row.date, row.time);
                    }
                    free(write_data);
                    continue;
                }
                
                write_data->use_bulk_mode = true;
            } else {
                // Incremental mode: calculate time index based on 2016-01-01 00:00:00
                int year = row.date / 10000;
                int month = (row.date / 100) % 100;
                int day = row.date % 100;
                int hour = row.time / 100;
                int minute = row.time % 100;
                
                // Calculate hours since 2016-01-01 00:00:00
                struct tm base_tm = {0};
                base_tm.tm_year = 2016 - 1900;
                base_tm.tm_mon = 0;
                base_tm.tm_mday = 1;
                
                struct tm curr_tm = {0};
                curr_tm.tm_year = year - 1900;
                curr_tm.tm_mon = month - 1;
                curr_tm.tm_mday = day;
                curr_tm.tm_hour = hour;
                curr_tm.tm_min = minute;
                
                time_t base_time = mktime(&base_tm);
                time_t curr_time = mktime(&curr_tm);
                
                if (base_time == -1 || curr_time == -1) {
                    if (data->verbose) {
                        fprintf(stderr, "Thread %d: Failed to calculate time for %u %u\n", 
                               data->thread_id, row.date, row.time);
                    }
                    free(write_data);
                    continue;
                }
                
                // Calculate time index (hours since base)
                time_idx = (size_t)((curr_time - base_time) / 3600);
                
                // Also track this timestamp for statistics
                find_or_add_timestamp(data->ctx, row.date, row.time);
                
                write_data->use_bulk_mode = false;
            }
            
            if (write_data->use_bulk_mode) {
                // Bulk mode: write directly to buffer in producer thread
                size_t buffer_offset = time_idx * MOBAKU_MESH_COUNT + mesh_idx;
                
                // Bounds check
                if (buffer_offset < (data->ctx->year_buffer_size / sizeof(int32_t))) {
                    data->ctx->year_buffer[buffer_offset] = row.population;
                } else {
                    if (data->verbose) {
                        fprintf(stderr, "Thread %d: Buffer offset %zu exceeds buffer size\n", 
                               data->thread_id, buffer_offset);
                    }
                }
                
                // No need to enqueue for bulk mode
                free(write_data);
                row_count++;
            } else {
                // Incremental mode: fill write data and enqueue for consumer
                write_data->time_index = time_idx;
                write_data->mesh_index = mesh_idx;
                write_data->population = row.population;
                
                // Enqueue the processed data
                enqueue(data->queue, write_data);
                row_count++;
            }
        }
        
        csv_close(reader);
        
        // Update statistics
        pthread_mutex_lock(data->stats_mutex);
        (*data->rows_processed) += row_count;
        (*data->total_files_processed)++;
        pthread_mutex_unlock(data->stats_mutex);
        
        // For bulk mode, also update converter statistics since consumer doesn't process
        if (data->ctx->use_bulk_write && data->ctx->year_buffer) {
            pthread_mutex_lock(&data->ctx->stats_mutex);
            data->ctx->stats.total_rows_processed += row_count;
            pthread_mutex_unlock(&data->ctx->stats_mutex);
        }
        
        // Update progress display
        if (data->verbose && data->total_files_processed) {
            pthread_mutex_lock(data->stats_mutex);
            size_t files_done = *data->total_files_processed;
            pthread_mutex_unlock(data->stats_mutex);
            display_progress(files_done, data->total_files, "CSV Processing");
        }
    }
    
    if (data->verbose) {
        printf("Enhanced CSV reader thread %d finished\n", data->thread_id);
    }
    return NULL;
}

int csv_to_h5_convert_file(const char* csv_filename, const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats) {
    const char* files[] = {csv_filename};
    return csv_to_h5_convert_files(files, 1, config, stats);
}


int csv_to_h5_convert_files(const char** csv_filenames, size_t num_files, 
                           const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats) {
    if (!csv_filenames || num_files == 0) return -1;
    
    csv_to_h5_config_t local_config = config ? *config : (csv_to_h5_config_t)CSV_TO_H5_DEFAULT_CONFIG;
    
    // Use multi-producer single-consumer pattern for all cases
    if (local_config.verbose) {
        printf("Processing %zu CSV files\n", num_files);
    }
    
    // Create converter context (handles both create and append modes)
    converter_ctx_t* ctx = converter_create(&local_config);
    if (!ctx) return -1;
    
    // Initialize FIFO queue
    FIFOQueue queue;
    init_queue(&queue);
    
    // Control variable for stopping consumer
    volatile int should_stop = 0;
    
    // Start consumer thread
    consumer_thread_data_t consumer_data = {
        .ctx = ctx,
        .queue = &queue,
        .config = &local_config,
        .should_stop = &should_stop
    };
    
    pthread_t consumer_thread;
    if (pthread_create(&consumer_thread, NULL, h5_consumer_thread_func, &consumer_data) != 0) {
        fprintf(stderr, "Failed to create consumer thread\n");
        converter_destroy(ctx);
        return -1;
    }
    
    // Determine number of producer threads (max 8, or 1 per 2 files)
    const int max_threads = 32;
    int num_threads = (int)num_files / 2;
    if (num_threads < 1) num_threads = 1;
    if (num_threads > max_threads) num_threads = max_threads;
    
    pthread_t* reader_threads = malloc(num_threads * sizeof(pthread_t));
    enhanced_csv_reader_thread_data_t* thread_data = malloc(num_threads * sizeof(enhanced_csv_reader_thread_data_t));
    
    if (!reader_threads || !thread_data) {
        fprintf(stderr, "Failed to allocate memory for threads\n");
        should_stop = 1;
        enqueue(&queue, NULL);
        pthread_join(consumer_thread, NULL);
        converter_destroy(ctx);
        free(reader_threads);
        free(thread_data);
        return -1;
    }
    
    // Distribute files among reader threads
    size_t files_per_thread = num_files / num_threads;
    size_t extra_files = num_files % num_threads;
    size_t file_index = 0;
    
    size_t total_rows_read = 0;
    size_t total_files_processed = 0;
    pthread_mutex_t reader_stats_mutex = PTHREAD_MUTEX_INITIALIZER;
    
    if (local_config.verbose) {
        printf("Starting %d CSV reader threads for %zu files\n", num_threads, num_files);
    }
    
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].queue = &queue;
        thread_data[i].filepaths = (char**)&csv_filenames[file_index];
        thread_data[i].num_files = files_per_thread + (i < extra_files ? 1 : 0);
        thread_data[i].rows_processed = &total_rows_read;
        thread_data[i].stats_mutex = &reader_stats_mutex;
        thread_data[i].ctx = ctx;  // Add converter context
        thread_data[i].verbose = local_config.verbose;
        thread_data[i].total_files_processed = &total_files_processed;
        thread_data[i].total_files = num_files;
        
        if (local_config.verbose) {
            printf("  Thread %d: %zu files (indices %zu-%zu)\n", 
                   i, thread_data[i].num_files, file_index, 
                   file_index + thread_data[i].num_files - 1);
        }
        
        if (pthread_create(&reader_threads[i], NULL, enhanced_csv_reader_thread_func, &thread_data[i]) != 0) {
            fprintf(stderr, "Failed to create reader thread %d\n", i);
            // Clean up and return error
            should_stop = 1;
            for (int j = 0; j < i; j++) {
                pthread_join(reader_threads[j], NULL);
            }
            enqueue(&queue, NULL);
            pthread_join(consumer_thread, NULL);
            converter_destroy(ctx);
            free(reader_threads);
            free(thread_data);
            return -1;
        }
        
        file_index += thread_data[i].num_files;
    }
    
    // Wait for all reader threads to finish
    if (local_config.verbose) {
        printf("Waiting for all reader threads to complete...\n");
    }
    
    for (int i = 0; i < num_threads; i++) {
        pthread_join(reader_threads[i], NULL);
    }
    
    if (local_config.verbose) {
        printf("All reader threads finished, signaling consumer to stop\n");
    }
    
    // Signal consumer to stop by enqueuing a NULL sentinel value
    //should_stop = 1;
    enqueue(&queue, NULL);
    
    // Wait for consumer thread
    pthread_join(consumer_thread, NULL);
    
    // Perform bulk write if enabled, then flush data
    if (perform_bulk_write(ctx, local_config.verbose) < 0) {
        // Clean up
        pthread_mutex_destroy(&reader_stats_mutex);
        free(reader_threads);
        free(thread_data);
        converter_destroy(ctx);
        return -1;
    }
    h5mobaku_flush(ctx->writer);
    
    // Final statistics
    if (stats) {
        pthread_mutex_lock(&ctx->stats_mutex);
        stats->total_rows_processed = ctx->stats.total_rows_processed;
        stats->unique_timestamps = ctx->timestamp_count;
        stats->unique_meshes = MOBAKU_MESH_COUNT;
        stats->errors = ctx->stats.errors;
        pthread_mutex_unlock(&ctx->stats_mutex);
    }
    
    if (local_config.verbose) {
        // Clear any remaining progress display
        if (isatty(STDOUT_FILENO)) {
            printf("\r\033[K");
        }
        
        pthread_mutex_lock(&ctx->stats_mutex);
        printf("Multi-threaded conversion completed:\n");
        printf("  Total rows processed: %zu\n", ctx->stats.total_rows_processed);
        printf("  Total files processed: %zu\n", total_files_processed);
        printf("  Unique timestamps: %zu\n", ctx->timestamp_count);
        printf("  Errors: %zu\n", ctx->stats.errors);
        pthread_mutex_unlock(&ctx->stats_mutex);
    }
    
    // Clean up
    pthread_mutex_destroy(&reader_stats_mutex);
    free(reader_threads);
    free(thread_data);
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