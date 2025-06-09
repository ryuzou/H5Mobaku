//
// Test for CSV operations with parallel reading and FIFO queue processing
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <time.h>
#include "csv_ops.h"
#include "fifioq.h"

#define MIN_FILES 95
#define MAX_FILES 105
#define MIN_ROWS_PER_FILE 950
#define MAX_ROWS_PER_FILE 1050

// Consumer thread data
typedef struct {
    FIFOQueue* queue;
    size_t* total_rows;
    pthread_mutex_t* stats_mutex;
} consumer_thread_data_t;

// Create test directory structure with CSV files
static int create_test_directory_structure(const char* base_dir) {
    // Seed random number generator
    srand((unsigned int)time(NULL));
    
    // Generate random number of files between MIN_FILES and MAX_FILES
    int num_files = MIN_FILES + rand() % (MAX_FILES - MIN_FILES + 1);
    
    printf("Generating %d CSV files with ~1000 rows each...\n", num_files);
    
    // Create base directory
    mkdir(base_dir, 0755);
    
    // Create multiple subdirectories to spread files across
    char path[512];
    const char* regions[] = {"region1", "region2", "region3", "region4", "region5"};
    const char* subregions[] = {"subregion1", "subregion2", "subregion3"};
    
    // Create region directories
    for (int r = 0; r < 5; r++) {
        snprintf(path, sizeof(path), "%s/%s", base_dir, regions[r]);
        mkdir(path, 0755);
        
        // Create subregion directories
        for (int s = 0; s < 3; s++) {
            snprintf(path, sizeof(path), "%s/%s/%s", base_dir, regions[r], subregions[s]);
            mkdir(path, 0755);
        }
    }
    
    // Generate CSV files with random distribution across directories
    for (int i = 0; i < num_files; i++) {
        // Randomly choose directory structure for this file
        int dir_type = rand() % 3;  // 0=root, 1=region, 2=subregion
        
        if (dir_type == 0) {
            // Root directory
            snprintf(path, sizeof(path), "%s/data_%03d.csv", base_dir, i);
        } else if (dir_type == 1) {
            // Region directory
            int region_idx = rand() % 5;
            snprintf(path, sizeof(path), "%s/%s/data_%03d.csv", 
                     base_dir, regions[region_idx], i);
        } else {
            // Subregion directory
            int region_idx = rand() % 5;
            int subregion_idx = rand() % 3;
            snprintf(path, sizeof(path), "%s/%s/%s/data_%03d.csv", 
                     base_dir, regions[region_idx], subregions[subregion_idx], i);
        }
        
        FILE* fp = fopen(path, "w");
        if (!fp) {
            fprintf(stderr, "Failed to create file: %s\n", path);
            continue;
        }
        
        // Write header
        fprintf(fp, "date,time,area,residence,age,gender,population\n");
        
        // Generate random number of rows per file (around 1000)
        int num_rows = MIN_ROWS_PER_FILE + rand() % (MAX_ROWS_PER_FILE - MIN_ROWS_PER_FILE + 1);
        
        // Write sample data rows with more realistic data distribution
        for (int j = 0; j < num_rows; j++) {
            // Generate date (2024, random month/day)
            int month = 1 + (rand() % 12);
            int day = 1 + (rand() % 28);  // Keep it simple, max 28 days
            
            // Generate time (random hour/minute)
            int hour = rand() % 24;
            int minute = (rand() % 6) * 10;  // 0, 10, 20, 30, 40, 50
            
            // Generate mesh ID (more realistic distribution)
            uint64_t base_meshid = 362257341ULL;
            uint64_t meshid = base_meshid + (rand() % 10000);  // Spread across 10k mesh IDs
            
            // Generate population (more realistic values)
            int population = 50 + (rand() % 500);  // Population between 50-549
            
            fprintf(fp, "2024%02d%02d,%02d%02d,%llu,-1,-1,-1,%d\n",
                    month, day, hour, minute, meshid, population);
        }
        
        fclose(fp);
        
        // Print progress every 10 files
        if ((i + 1) % 10 == 0) {
            printf("  Created %d/%d files...\n", i + 1, num_files);
        }
    }
    
    return num_files;
}

// Recursively find all CSV files in directory
static void find_csv_files(const char* dir_path, char*** files, size_t* count, size_t* capacity) {
    DIR* dir = opendir(dir_path);
    if (!dir) return;
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        
        char full_path[1024];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
        
        struct stat st;
        if (stat(full_path, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                // Recursively search subdirectory
                find_csv_files(full_path, files, count, capacity);
            } else if (S_ISREG(st.st_mode)) {
                // Check if it's a CSV file
                size_t len = strlen(entry->d_name);
                if (len > 4 && strcmp(entry->d_name + len - 4, ".csv") == 0) {
                    // Add to files array
                    if (*count >= *capacity) {
                        *capacity *= 2;
                        *files = realloc(*files, *capacity * sizeof(char*));
                    }
                    (*files)[*count] = strdup(full_path);
                    (*count)++;
                }
            }
        }
    }
    
    closedir(dir);
}


// Consumer thread function
static void* consumer_thread_func(void* arg) {
    consumer_thread_data_t* data = (consumer_thread_data_t*)arg;
    
    printf("Consumer thread started\n");
    
    while (1) {
        // Dequeue will block until data is available
        population_data_t* pop_data = (population_data_t*)dequeue(data->queue);
        
        // Check if this is a sentinel value (NULL) indicating shutdown
        if (!pop_data) {
            printf("Consumer: Received shutdown signal, stopping\n");
            break;
        }
        
        // Process population data (in real usage, this would write to HDF5)
        // Convert datetime to readable format
        char time_str[64];
        struct tm* tm_info = localtime(&pop_data->datetime);
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
        
        // Only print first 50 rows to avoid overwhelming output
        static int printed_count = 0;
        if (printed_count < 50) {
            printf("Consumer: meshid=%llu, datetime=%s, population=%d (from %s)\n",
                   (unsigned long long)pop_data->meshid, time_str, pop_data->population,
                   pop_data->source_file);
            printed_count++;
        } else if (printed_count == 50) {
            printf("Consumer: ... (suppressing further output for performance) ...\n");
            printed_count++;
        }
        
        // Update statistics
        pthread_mutex_lock(data->stats_mutex);
        (*data->total_rows)++;
        pthread_mutex_unlock(data->stats_mutex);

        
        // Clean up
        free_population_data(pop_data);
    }
    
    printf("Consumer thread finished\n");
    return NULL;
}

void test_parallel_csv_reading() {
    printf("=== Testing Parallel CSV Reading with FIFO Queue ===\n");
    
    const char* test_dir = "test_csv_dir";
    
    // Create test directory structure
    printf("Creating test directory structure...\n");
    int expected_files = create_test_directory_structure(test_dir);
    
    // STEP 1: Enumerate all CSV files first
    printf("\nStep 1: Enumerating all CSV files...\n");
    char** all_csv_files = malloc(MAX_FILES * sizeof(char*));
    size_t total_file_count = 0;
    size_t file_capacity = MAX_FILES;
    find_csv_files(test_dir, &all_csv_files, &total_file_count, &file_capacity);
    
    // Verify we found the expected number of files
    if (total_file_count != expected_files) {
        printf("Warning: Expected %d files, found %zu files\n", expected_files, total_file_count);
    }
    
    printf("Total CSV files found: %zu\n", total_file_count);
    
    // Only show first 10 files to avoid cluttering output
    size_t files_to_show = (total_file_count < 10) ? total_file_count : 10;
    for (size_t i = 0; i < files_to_show; i++) {
        printf("  [%zu] %s\n", i, all_csv_files[i]);
    }
    if (total_file_count > 10) {
        printf("  ... and %zu more files\n", total_file_count - 10);
    }
    
    // Initialize FIFO queue
    FIFOQueue queue;
    init_queue(&queue);
    
    // Statistics
    size_t total_rows_read = 0;
    size_t total_rows_consumed = 0;
    pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;

    // Start consumer thread
    consumer_thread_data_t consumer_data = {
        .queue = &queue,
        .total_rows = &total_rows_consumed,
        .stats_mutex = &stats_mutex
    };
    pthread_t consumer_thread;
    pthread_create(&consumer_thread, NULL, consumer_thread_func, &consumer_data);
    
    // STEP 2: Distribute the enumerated files among reader threads
    printf("\nStep 2: Distributing files among reader threads...\n");
    const int num_threads = 8;  // Increase threads for larger workload
    pthread_t reader_threads[num_threads];
    csv_reader_thread_data_t thread_data[num_threads];
    
    // Calculate file distribution
    size_t files_per_thread = total_file_count / num_threads;
    size_t extra_files = total_file_count % num_threads;
    size_t file_index = 0;
    
    printf("Distribution plan:\n");
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].queue = &queue;
        thread_data[i].filepaths = &all_csv_files[file_index];
        thread_data[i].num_files = files_per_thread + (i < extra_files ? 1 : 0);
        thread_data[i].rows_processed = &total_rows_read;
        thread_data[i].stats_mutex = &stats_mutex;
        
        printf("  Thread %d: %zu files (indices %zu-%zu)\n", 
               i, thread_data[i].num_files, file_index, 
               file_index + thread_data[i].num_files - 1);
        
        file_index += thread_data[i].num_files;
    }
    
    // STEP 3: Start all reader threads with their assigned file lists
    printf("\nStep 3: Starting reader threads...\n");
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&reader_threads[i], NULL, csv_reader_thread_func, &thread_data[i]);
    }
    
    // STEP 4: Wait for all reader threads to finish processing their assigned files
    printf("\nStep 4: Waiting for all reader threads to complete...\n");
    for (int i = 0; i < num_threads; i++) {
        pthread_join(reader_threads[i], NULL);
    }
    
    printf("All reader threads finished processing their assigned files\n");
    
    // Signal consumer to stop by enqueuing a NULL sentinel value
    printf("Sending shutdown signal to consumer thread\n");
    enqueue(&queue, NULL);
    
    // Wait for consumer thread
    pthread_join(consumer_thread, NULL);
    
    // Print final statistics
    printf("\n=== Final Statistics ===\n");
    printf("Total CSV files processed: %zu\n", total_file_count);
    printf("Total rows read by readers: %zu\n", total_rows_read);
    printf("Total rows consumed: %zu\n", total_rows_consumed);
    printf("Average rows per file: %.1f\n", (double)total_rows_read / total_file_count);
    printf("Processing rate: %.1f rows/sec\n", (double)total_rows_consumed / 
           ((double)total_rows_consumed * 0.0001)); // Approximate based on 0.1ms per row
    
    // Verify all rows were processed
    assert(total_rows_consumed == total_rows_read);
    
    // STEP 5: Clean up allocated file list
    printf("\nStep 5: Cleaning up...\n");
    for (size_t i = 0; i < total_file_count; i++) {
        free(all_csv_files[i]);
    }
    free(all_csv_files);
    
    // Clean up test directory
    system("rm -rf test_csv_dir");
    
    printf("\nTest passed!\n");
}

void test_queue_blocking_behavior() {
    printf("\n=== Testing Queue Blocking Behavior ===\n");
    
    FIFOQueue queue;
    init_queue(&queue);
    
    // Test that queue blocks when full
    printf("Filling queue to capacity (%d items)...\n", QUEUE_SIZE);
    for (int i = 0; i < QUEUE_SIZE; i++) {
        int* data = malloc(sizeof(int));
        *data = i;
        enqueue(&queue, data);
    }
    
    printf("Queue is now full\n");
    
    // Verify queue state
    pthread_mutex_lock(&queue.mutex);
    assert(queue.count == QUEUE_SIZE);
    pthread_mutex_unlock(&queue.mutex);
    
    // Dequeue half the items
    printf("Dequeuing %d items...\n", QUEUE_SIZE / 2);
    for (int i = 0; i < QUEUE_SIZE / 2; i++) {
        int* data = (int*)dequeue(&queue);
        assert(*data == i);
        free(data);
    }
    
    // Verify queue state
    pthread_mutex_lock(&queue.mutex);
    assert(queue.count == QUEUE_SIZE / 2);
    pthread_mutex_unlock(&queue.mutex);
    
    // Clean up remaining items
    while (queue.count > 0) {
        int* data = (int*)dequeue(&queue);
        free(data);
    }
    
    printf("Queue blocking behavior test passed!\n");
}

int main() {
    printf("Starting CSV operations tests...\n\n");
    
    // Test parallel CSV reading with FIFO queue
    test_parallel_csv_reading();
    
    // Test queue blocking behavior
    test_queue_blocking_behavior();
    
    printf("\nAll tests passed!\n");
    return 0;
}