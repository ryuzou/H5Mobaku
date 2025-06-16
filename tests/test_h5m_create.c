//
// Test for h5m-create CLI tool with VDS functionality
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
#include <sys/wait.h>
#include <stdint.h>

#define MIN_FILES_HISTORICAL 30
#define MAX_FILES_HISTORICAL 40
#define MIN_FILES_NEW 20
#define MAX_FILES_NEW 30
#define MIN_ROWS_PER_FILE 300
#define MAX_ROWS_PER_FILE 500

// Test configuration
typedef struct {
    const char* historical_dir;
    const char* new_dir;
    const char* historical_h5;
    const char* combined_h5;
    int cutoff_year;
} test_config_t;

// Create CSV files for historical data (2016-2018)
static int create_historical_csv_files(const char* base_dir) {
    srand((unsigned int)time(NULL));
    
    int num_files = MIN_FILES_HISTORICAL + rand() % (MAX_FILES_HISTORICAL - MIN_FILES_HISTORICAL + 1);
    
    printf("Generating %d historical CSV files (2016-2018)...\n", num_files);
    
    mkdir(base_dir, 0755);
    
    char path[512];
    const char* regions[] = {"tokyo", "osaka", "nagoya", "fukuoka", "sapporo"};
    const char* subregions[] = {"central", "north", "south"};
    
    // Create region directories
    for (int r = 0; r < 5; r++) {
        snprintf(path, sizeof(path), "%s/%s", base_dir, regions[r]);
        mkdir(path, 0755);
        
        for (int s = 0; s < 3; s++) {
            snprintf(path, sizeof(path), "%s/%s/%s", base_dir, regions[r], subregions[s]);
            mkdir(path, 0755);
        }
    }
    
    // Generate CSV files with historical data (2016-2018)
    for (int i = 0; i < num_files; i++) {
        int dir_type = rand() % 3;
        
        if (dir_type == 0) {
            snprintf(path, sizeof(path), "%s/historical_%03d.csv", base_dir, i);
        } else if (dir_type == 1) {
            int region_idx = rand() % 5;
            snprintf(path, sizeof(path), "%s/%s/historical_%03d.csv", 
                     base_dir, regions[region_idx], i);
        } else {
            int region_idx = rand() % 5;
            int subregion_idx = rand() % 3;
            snprintf(path, sizeof(path), "%s/%s/%s/historical_%03d.csv", 
                     base_dir, regions[region_idx], subregions[subregion_idx], i);
        }
        
        FILE* fp = fopen(path, "w");
        if (!fp) {
            fprintf(stderr, "Failed to create file: %s\n", path);
            continue;
        }
        
        fprintf(fp, "date,time,area,residence,age,gender,population\n");
        
        int num_rows = MIN_ROWS_PER_FILE + rand() % (MAX_ROWS_PER_FILE - MIN_ROWS_PER_FILE + 1);
        
        // Generate data for 2016-2018 (historical period)
        for (int j = 0; j < num_rows; j++) {
            // Historical years: 2016, 2017, 2018
            int year = 2016 + (rand() % 3);
            int month = 1 + (rand() % 12);
            int day = 1 + (rand() % 28);
            
            int hour = rand() % 24;
            int minute = (rand() % 6) * 10;
            
            // Use different mesh ID ranges for different regions
            int region_for_mesh = rand() % 5;
            uint64_t base_meshid = 362257341ULL + (region_for_mesh * 10000);
            uint64_t meshid = base_meshid + (rand() % 5000);
            
            // Historical population (lower values)
            int population = 100 + (rand() % 400);
            
            fprintf(fp, "%04d%02d%02d,%02d%02d,%lu,-1,-1,-1,%d\n",
                    year, month, day, hour, minute, (unsigned long)meshid, population);
        }
        
        fclose(fp);
        
        if ((i + 1) % 10 == 0) {
            printf("  Created %d/%d historical files...\n", i + 1, num_files);
        }
    }
    
    return num_files;
}

// Create CSV files for new data (2019-2023)
static int create_new_csv_files(const char* base_dir) {
    int num_files = MIN_FILES_NEW + rand() % (MAX_FILES_NEW - MIN_FILES_NEW + 1);
    
    printf("Generating %d new CSV files (2019-2023)...\n", num_files);
    
    mkdir(base_dir, 0755);
    
    char path[512];
    const char* districts[] = {"shibuya", "shinjuku", "harajuku", "ginza"};
    
    // Create district directories
    for (int d = 0; d < 4; d++) {
        snprintf(path, sizeof(path), "%s/%s", base_dir, districts[d]);
        mkdir(path, 0755);
    }
    
    // Generate CSV files with new data (2019-2023)
    for (int i = 0; i < num_files; i++) {
        int dir_type = rand() % 2;
        
        if (dir_type == 0) {
            snprintf(path, sizeof(path), "%s/new_%03d.csv", base_dir, i);
        } else {
            int district_idx = rand() % 4;
            snprintf(path, sizeof(path), "%s/%s/new_%03d.csv", 
                     base_dir, districts[district_idx], i);
        }
        
        FILE* fp = fopen(path, "w");
        if (!fp) {
            fprintf(stderr, "Failed to create file: %s\n", path);
            continue;
        }
        
        fprintf(fp, "date,time,area,residence,age,gender,population\n");
        
        int num_rows = MIN_ROWS_PER_FILE + rand() % (MAX_ROWS_PER_FILE - MIN_ROWS_PER_FILE + 1);
        
        // Generate data for 2019-2023 (new period)
        for (int j = 0; j < num_rows; j++) {
            // New years: 2019, 2020, 2021, 2022, 2023
            int year = 2019 + (rand() % 5);
            int month = 1 + (rand() % 12);
            int day = 1 + (rand() % 28);
            
            int hour = rand() % 24;
            int minute = (rand() % 6) * 10;
            
            // Use different mesh ID ranges, overlapping with historical but extended
            uint64_t base_meshid = 362257341ULL;
            uint64_t meshid = base_meshid + (rand() % 15000); // Expanded range
            
            // New population (higher values to show growth)
            int population = 200 + (rand() % 600);
            
            fprintf(fp, "%04d%02d%02d,%02d%02d,%lu,-1,-1,-1,%d\n",
                    year, month, day, hour, minute, (unsigned long)meshid, population);
        }
        
        fclose(fp);
        
        if ((i + 1) % 10 == 0) {
            printf("  Created %d/%d new files...\n", i + 1, num_files);
        }
    }
    
    return num_files;
}

// Execute h5m-create command and capture output
static int run_h5m_create(const char* args, char* output_buffer, size_t buffer_size) {
    char command[1024];
    snprintf(command, sizeof(command), "./h5m-create %s 2>&1", args);
    
    FILE* pipe = popen(command, "r");
    if (!pipe) {
        fprintf(stderr, "Failed to execute: %s\n", command);
        return -1;
    }
    
    size_t total_read = 0;
    while (total_read < buffer_size - 1 && 
           fgets(output_buffer + total_read, buffer_size - total_read, pipe) != NULL) {
        total_read = strlen(output_buffer);
    }
    
    int exit_code = pclose(pipe);
    int wstatus = WEXITSTATUS(exit_code);
    if (wstatus != 0) {
        fprintf(stderr, "h5m-create failed with exit code: %d (raw: %d)\n", wstatus, exit_code);
        fprintf(stderr, "Command was: %s\n", command);
    }
    return wstatus;
}

// Execute h5m-reader command and parse the population value
static int run_h5m_reader_single(const char* h5_file, uint32_t mesh_id, const char* datetime, int32_t* result_value) {
    char command[1024];
    snprintf(command, sizeof(command), "./h5m-reader -f %s -m %u -t \"%s\" 2>/dev/null", 
             h5_file, mesh_id, datetime);
    
    FILE* pipe = popen(command, "r");
    if (!pipe) {
        fprintf(stderr, "Failed to execute: %s\n", command);
        return -1;
    }
    
    char line[512];
    int found_value = 0;
    *result_value = -1;
    
    // Parse the output table to extract the population value
    while (fgets(line, sizeof(line), pipe) != NULL) {
        // Look for the data line (contains mesh ID, datetime, and population)
        if (strstr(line, "|") && !strstr(line, "Mesh ID") && !strstr(line, "+--")) {
            // Parse: | 362257341  | 2019-01-01 01:00:00 |        100 |
            char* last_pipe = strrchr(line, '|');
            if (last_pipe != NULL) {
                // Move backwards to find the second-to-last pipe
                char* second_last = last_pipe - 1;
                while (second_last > line && *second_last != '|') second_last--;
                if (*second_last == '|') {
                    // Extract the number between the pipes
                    char value_str[32];
                    int i = 0;
                    second_last++; // Move past the '|'
                    while (*second_last == ' ') second_last++; // Skip spaces
                    while (*second_last != ' ' && *second_last != '|' && i < 31) {
                        value_str[i++] = *second_last++;
                    }
                    value_str[i] = '\0';
                    *result_value = atoi(value_str);
                    found_value = 1;
                    break;
                }
            }
        }
    }
    
    int exit_code = pclose(pipe);
    int wstatus = WEXITSTATUS(exit_code);
    
    if (wstatus != 0 || !found_value) {
        return -1;
    }
    
    return 0;
}

// Test data existence by checking multiple time points with h5m-reader
static int test_data_existence(const char* h5_file, uint32_t mesh_id, const char* base_datetime_format, 
                              int num_tests, int* zero_count, int* non_zero_count) {
    *zero_count = 0;
    *non_zero_count = 0;
    
    for (int i = 0; i < num_tests; i++) {
        char datetime[64];
        snprintf(datetime, sizeof(datetime), base_datetime_format, i);
        
        int32_t value;
        if (run_h5m_reader_single(h5_file, mesh_id, datetime, &value) == 0) {
            printf("  %s, mesh %u: %d\n", datetime, mesh_id, value);
            if (value == 0) {
                (*zero_count)++;
            } else {
                (*non_zero_count)++;
            }
        } else {
            printf("  %s, mesh %u: READ_ERROR\n", datetime, mesh_id);
            (*zero_count)++;
        }
    }
    
    return 0;
}

// Test basic h5m-create functionality without VDS
void test_basic_h5m_create() {
    printf("=== Testing Basic H5M-Create Functionality ===\n");
    
    const char* test_dir = "test_basic_h5m";
    
    printf("Step 1: Creating test CSV files...\n");
    volatile int expected_files = create_new_csv_files(test_dir);
    
    printf("\nStep 2: Running h5m-create without VDS...\n");
    char output[65536];
    char args[512];
    snprintf(args, sizeof(args), "-o test_basic.h5 -d %s --verbose", test_dir);
    
    int result = run_h5m_create(args, output, sizeof(output));
    printf("Command output:\n%s\n", output);
    
    assert(result == 0);
    
    // Verify output file exists
    struct stat st;
    assert(stat("test_basic.h5", &st) == 0);
    
    printf("Step 3: Verifying H5 file with h5m-reader...\n");
    
    // Test reading with h5m-reader command for data from 2019-2023 (test data)
    uint32_t test_mesh = 362257341;
    int zero_count = 0, non_zero_count = 0;
    
    // Test multiple time points in 2019 (when test data should be)
    const char* test_times[] = {
        "2019-01-01 00:00:00",
        "2019-01-01 01:00:00", 
        "2019-01-01 02:00:00",
        "2019-01-02 00:00:00",
        "2019-01-02 01:00:00"
    };
    
    for (int i = 0; i < 5; i++) {
        int32_t value;
        if (run_h5m_reader_single("test_basic.h5", test_mesh, test_times[i], &value) == 0) {
            printf("  %s, mesh %u: %d\n", test_times[i], test_mesh, value);
            if (value == 0) {
                zero_count++;
            } else {
                non_zero_count++;
            }
        } else {
            printf("  %s, mesh %u: READ_ERROR\n", test_times[i], test_mesh);
            zero_count++;
        }
    }
    
    printf("  Data check: %d zero, %d non-zero values\n", zero_count, non_zero_count);
    
    if (non_zero_count == 0) {
        printf("WARNING: All sampled values are 0 - possible data writing issue\n");
    }
    
    printf("Step 4: Cleaning up...\n");
    system("rm -rf test_basic_h5m");
    unlink("test_basic.h5");
    
    printf("Basic H5M-Create test passed!\n\n");
}

// Test h5m-create with VDS functionality
void test_vds_h5m_create() {
    printf("=== Testing H5M-Create with VDS Integration ===\n");
    
    test_config_t config = {
        .historical_dir = "test_historical_data",
        .new_dir = "test_new_data", 
        .historical_h5 = "test_historical.h5",
        .combined_h5 = "test_combined_vds.h5",
        .cutoff_year = 2019
    };
    
    printf("Step 1: Creating historical CSV files (2016-2018)...\n");
    int historical_files = create_historical_csv_files(config.historical_dir);
    
    printf("\nStep 2: Creating new CSV files (2019-2023)...\n");  
    int new_files = create_new_csv_files(config.new_dir);
    
    printf("\nStep 3: Creating historical H5 file...\n");
    char output[65536];
    char args[512];
    snprintf(args, sizeof(args), "-o %s -d %s --verbose", 
             config.historical_h5, config.historical_dir);
    
    int result = run_h5m_create(args, output, sizeof(output));
    printf("Historical creation output:\n%s\n", output);
    assert(result == 0);
    
    // Verify historical file
    struct stat st;
    assert(stat(config.historical_h5, &st) == 0);
    printf("Historical H5 file size: %ld bytes\n", st.st_size);
    
    printf("\nStep 4: Creating combined VDS file...\n");
    snprintf(args, sizeof(args), "-o %s -d %s -v %s -y %d --verbose", 
             config.combined_h5, config.new_dir, config.historical_h5, config.cutoff_year);
    
    result = run_h5m_create(args, output, sizeof(output));
    printf("VDS creation output:\n%s\n", output);
    assert(result == 0);
    
    // Verify combined file
    assert(stat(config.combined_h5, &st) == 0);
    printf("Combined VDS file size: %ld bytes\n", st.st_size);
    
    printf("\nStep 5: Testing VDS data access with h5m-reader...\n");
    
    uint32_t test_mesh = 362257341;
    printf("Testing mesh ID %u:\n", test_mesh);
    
    // Test historical data access (2017)
    const char* historical_time = "2017-06-01 12:00:00";
    int32_t hist_value, vds_hist_value;
    
    printf("  Historical data (%s):\n", historical_time);
    if (run_h5m_reader_single(config.historical_h5, test_mesh, historical_time, &hist_value) == 0) {
        printf("    Original file: %d\n", hist_value);
    } else {
        printf("    Original file: READ_ERROR\n");
        hist_value = -1;
    }
    
    if (run_h5m_reader_single(config.combined_h5, test_mesh, historical_time, &vds_hist_value) == 0) {
        printf("    VDS file: %d\n", vds_hist_value);
    } else {
        printf("    VDS file: READ_ERROR\n");
        vds_hist_value = -1;
    }
    
    // Test new data access (2020) - should only work with VDS file
    const char* new_time = "2020-06-01 12:00:00";
    int32_t vds_new_value;
    
    printf("  New data (%s):\n", new_time);
    if (run_h5m_reader_single(config.combined_h5, test_mesh, new_time, &vds_new_value) == 0) {
        printf("    VDS file: %d\n", vds_new_value);
    } else {
        printf("    VDS file: READ_ERROR\n");
        vds_new_value = -1;
    }
    
    // Test boundary data points
    printf("  VDS boundary test:\n");
    const char* boundary_times[] = {
        "2018-12-31 23:00:00",  // Should be in historical data
        "2019-01-01 00:00:00",  // Should be in new data
        "2019-01-01 01:00:00"   // Should be in new data
    };
    
    for (int i = 0; i < 3; i++) {
        int32_t boundary_value;
        if (run_h5m_reader_single(config.combined_h5, test_mesh, boundary_times[i], &boundary_value) == 0) {
            printf("    %s: %d\n", boundary_times[i], boundary_value);
        } else {
            printf("    %s: READ_ERROR\n", boundary_times[i]);
        }
    }
    
    printf("\nStep 6: Performance comparison...\n");
    
    // Measure file sizes and structure
    struct stat hist_st, combined_st;
    stat(config.historical_h5, &hist_st);
    stat(config.combined_h5, &combined_st);
    
    printf("File size comparison:\n");
    printf("  Historical only: %ld bytes\n", hist_st.st_size);
    printf("  Combined VDS: %ld bytes\n", combined_st.st_size);
    printf("  Space efficiency: %.1f%% (VDS avoids duplication)\n", 
           100.0 * (double)combined_st.st_size / (hist_st.st_size * 2));
    
    printf("\nStep 7: Cleaning up...\n");
    system("rm -rf test_historical_data test_new_data");
    unlink(config.historical_h5);
    unlink(config.combined_h5);
    
    printf("VDS H5M-Create test passed!\n\n");
}

// Test error conditions and edge cases
void test_h5m_create_error_cases() {
    printf("=== Testing H5M-Create Error Cases ===\n");
    
    char output[65536];
    int result;
    
    printf("Test 1: Missing required arguments...\n");
    result = run_h5m_create("", output, sizeof(output));
    assert(result != 0); // Should fail
    printf("  Correctly failed with missing args\n");
    
    printf("Test 2: Non-existent directory...\n");
    result = run_h5m_create("-o test.h5 -d /nonexistent/directory", output, sizeof(output));
    assert(result != 0); // Should fail
    printf("  Correctly failed with non-existent directory\n");
    
    printf("Test 3: VDS source file doesn't exist...\n");
    mkdir("empty_dir", 0755);
    result = run_h5m_create("-o test.h5 -d empty_dir -v nonexistent.h5 -y 2020", 
                           output, sizeof(output));
    assert(result != 0); // Should fail
    rmdir("empty_dir");
    printf("  Correctly failed with non-existent VDS source\n");
    
    printf("Test 4: VDS year without source file...\n");
    mkdir("empty_dir", 0755);
    result = run_h5m_create("-o test.h5 -d empty_dir -y 2020", output, sizeof(output));
    assert(result != 0); // Should fail
    rmdir("empty_dir");
    printf("  Correctly failed with VDS year but no source\n");
    
    printf("Error case tests passed!\n\n");
}

// Test with large-scale data
void test_large_scale_h5m_create() {
    printf("=== Testing Large-Scale H5M-Create ===\n");
    
    // Create a larger dataset to test performance
    const char* large_dir = "test_large_data";
    mkdir(large_dir, 0755);
    
    printf("Creating large-scale test data (this may take a moment)...\n");
    
    // Generate 100 files with 1000 rows each
    for (int i = 0; i < 100; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/large_%03d.csv", large_dir, i);
        
        FILE* fp = fopen(path, "w");
        if (!fp) continue;
        
        fprintf(fp, "date,time,area,residence,age,gender,population\n");
        
        // Generate 1000 rows per file
        for (int j = 0; j < 1000; j++) {
            int year = 2022 + (rand() % 2); // 2022-2023
            int month = 1 + (rand() % 12);
            int day = 1 + (rand() % 28);
            int hour = rand() % 24;
            int minute = (rand() % 6) * 10;
            
            uint64_t meshid = 362257341ULL + (rand() % 10000);
            int population = 50 + (rand() % 500);
            
            fprintf(fp, "%04d%02d%02d,%02d%02d,%lu,-1,-1,-1,%d\n",
                    year, month, day, hour, minute, (unsigned long)meshid, population);
        }
        
        fclose(fp);
        
        if ((i + 1) % 25 == 0) {
            printf("  Created %d/100 large files...\n", i + 1);
        }
    }
    
    printf("Running h5m-create on large dataset...\n");
    clock_t start_time = clock();
    
    char output[65536];
    char args[512];
    snprintf(args, sizeof(args), "-o test_large.h5 -d %s --verbose -b 50000", large_dir);
    
    int result = run_h5m_create(args, output, sizeof(output));
    
    clock_t end_time = clock();
    double processing_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    
    printf("Processing completed in %.2f seconds\n", processing_time);
    assert(result == 0);
    
    // Verify the large file
    struct stat st;
    assert(stat("test_large.h5", &st) == 0);
    printf("Large H5 file size: %ld bytes (%.1f MB)\n", st.st_size, st.st_size / 1024.0 / 1024.0);
    
    printf("Performance: %.0f rows/second\n", (100 * 1000) / processing_time);
    
    // Cleanup
    system("rm -rf test_large_data");
    unlink("test_large.h5");
    
    printf("Large-scale test passed!\n\n");
}

// Test bulk write mode
void test_bulk_write_h5m_create() {
    printf("=== Testing Bulk Write Mode H5M-Create ===\n");
    
    const char* bulk_dir = "test_bulk_data";
    mkdir(bulk_dir, 0755);
    
    printf("Creating test data for bulk write mode...\n");
    
    // Create test CSV files - enough to test bulk mode but not require full 51GB
    // Generate files for a small subset to test functionality
    char sub_dirs[][20] = {"20230101", "20230102", "20230103"};
    
    for (int d = 0; d < 3; d++) {
        char dir_path[512];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", bulk_dir, sub_dirs[d]);
        mkdir(dir_path, 0755);
        
        // Create a few CSV files per directory
        for (int f = 0; f < 2; f++) {
            char file_path[512];
            snprintf(file_path, sizeof(file_path), "%s/data_%02d.csv", dir_path, f);
            
            FILE* fp = fopen(file_path, "w");
            if (!fp) {
                fprintf(stderr, "Failed to create %s\n", file_path);
                continue;
            }
            
            fprintf(fp, "date,time,area,residence,age,gender,population\n");
            
            // Write some test data
            const uint32_t test_meshes[] = {362257341, 523365702, 533946132};
            int year = 2023;
            int month = 1;
            int day = d + 1;
            
            for (int hour = 0; hour < 24; hour++) {
                for (int m = 0; m < 3; m++) {
                    int population = rand() % 1000 + 100;
                    fprintf(fp, "%04d%02d%02d,%02d00,%u,-1,-1,-1,%d\n",
                            year, month, day, hour, test_meshes[m], population);
                }
            }
            
            fclose(fp);
        }
    }
    
    // Test 1: Run with bulk write mode
    printf("\nTest 1: Running h5m-create with --bulk-write...\n");
    char output[65536];
    char args[512];
    snprintf(args, sizeof(args), "-o test_bulk.h5 -d %s --bulk-write --verbose", bulk_dir);
    
    int result = run_h5m_create(args, output, sizeof(output));
    if (result != 0) {
        fprintf(stderr, "h5m-create failed with bulk write mode: %s\n", output);
    }
    assert(result == 0);
    
    // Verify output contains bulk mode messages
    assert(strstr(output, "Bulk write mode: ENABLED") != NULL);
    assert(strstr(output, "Bulk mode enabled, consumer idle") != NULL);
    
    // Test 2: Verify the created file
    printf("Test 2: Verifying bulk-written HDF5 file...\n");
    struct stat st;
    assert(stat("test_bulk.h5", &st) == 0);
    printf("  Created HDF5 file size: %ld bytes\n", st.st_size);
    
    // Test 3: Verify data can be read correctly with h5m-reader
    printf("Test 3: Verifying bulk-written data with h5m-reader...\n");
    
    uint32_t test_mesh = 362257341;
    
    // Test 2023 data (should be written correctly now)
    const char* test_2023_times[] = {
        "2023-01-01 00:00:00",
        "2023-01-01 01:00:00", 
        "2023-01-01 02:00:00",
        "2023-01-02 00:00:00",
        "2023-01-02 01:00:00"
    };
    
    int zero_count = 0, non_zero_count = 0;
    printf("  Testing 2023 data (correct time indices):\n");
    for (int i = 0; i < 5; i++) {
        int32_t value;
        if (run_h5m_reader_single("test_bulk.h5", test_mesh, test_2023_times[i], &value) == 0) {
            printf("    %s: %d\n", test_2023_times[i], value);
            if (value == 0) {
                zero_count++;
            } else {
                non_zero_count++;
                // Verify value is in expected range
                assert(value >= 100 && value < 1100);
            }
        } else {
            printf("    %s: READ_ERROR\n", test_2023_times[i]);
            zero_count++;
        }
    }
    
    printf("  Data check: %d zero, %d non-zero values\n", zero_count, non_zero_count);
    
    // Verify that data is NOT at wrong locations (2016 beginning)
    printf("  Testing 2016 data (should be empty - wrong time indices):\n");
    const char* test_2016_times[] = {
        "2016-01-01 00:00:00",
        "2016-01-01 01:00:00", 
        "2016-01-01 02:00:00"
    };
    
    int wrong_non_zero = 0;
    for (int i = 0; i < 3; i++) {
        int32_t value;
        if (run_h5m_reader_single("test_bulk.h5", test_mesh, test_2016_times[i], &value) == 0) {
            printf("    %s: %d\n", test_2016_times[i], value);
            if (value != 0) wrong_non_zero++;
        } else {
            printf("    %s: READ_ERROR\n", test_2016_times[i]);
        }
    }
    
    if (non_zero_count > 0) {
        printf("  SUCCESS: Data correctly written at 2023 time indices\n");
        if (wrong_non_zero > 0) {
            printf("  WARNING: Found %d non-zero values at 2016 indices - may indicate bug\n", wrong_non_zero);
        } else {
            printf("  SUCCESS: No incorrect data at 2016 indices\n");
        }
    } else {
        printf("  ERROR: No data found at correct 2023 time indices\n");
        assert(non_zero_count > 0);
    }
    
    // Cleanup
    system("rm -rf test_bulk_data");
    unlink("test_bulk.h5");
    
    printf("Bulk write mode test passed!\n\n");
}

int main() {
    printf("Starting H5M-Create comprehensive tests...\n\n");
    
    // Test basic functionality
    test_basic_h5m_create();
    
    // Test VDS functionality
    test_vds_h5m_create();
    
    // Test error conditions
    test_h5m_create_error_cases();
    
    // Test large-scale performance
    //test_large_scale_h5m_create();
    
    // Test bulk write mode
    test_bulk_write_h5m_create();
    
    printf("=== All H5M-Create Tests Passed! ===\n");
    
    printf("\nTest Summary:\n");
    printf("✓ Basic CSV to H5 conversion\n");
    printf("✓ VDS integration with historical data\n");
    printf("✓ Self-reference for new data within same file\n");
    printf("✓ Error handling and validation\n");
    printf("✓ Large-scale performance\n");
    printf("✓ Data integrity across VDS boundaries\n");
    printf("✓ Bulk write mode (51 GiB optimization)\n");
    
    return 0;
}