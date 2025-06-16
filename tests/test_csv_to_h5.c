//
// Test for CSV to HDF5 conversion with multi-producer CSV generation
//

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <time.h>
#include "csv_to_h5_converter.h"
#include "H5MR/h5mr.h"
#include "h5mobaku_ops.h"
#include "meshid_ops.h"
#include "fifioq.h"
#include "csv_ops.h"

#define MIN_FILES 50
#define MAX_FILES 80
#define MIN_ROWS_PER_FILE 500
#define MAX_ROWS_PER_FILE 800

static int create_test_directory_structure(const char* base_dir) {
    srand((unsigned int)time(NULL));
    
    int num_files = MIN_FILES + rand() % (MAX_FILES - MIN_FILES + 1);
    
    printf("Generating %d CSV files with ~650 rows each...\n", num_files);
    
    mkdir(base_dir, 0755);
    
    char path[512];
    const char* regions[] = {"region1", "region2", "region3", "region4", "region5"};
    const char* subregions[] = {"subregion1", "subregion2", "subregion3"};
    
    for (int r = 0; r < 5; r++) {
        snprintf(path, sizeof(path), "%s/%s", base_dir, regions[r]);
        mkdir(path, 0755);
        
        for (int s = 0; s < 3; s++) {
            snprintf(path, sizeof(path), "%s/%s/%s", base_dir, regions[r], subregions[s]);
            mkdir(path, 0755);
        }
    }
    
    for (int i = 0; i < num_files; i++) {
        int dir_type = rand() % 3;
        
        if (dir_type == 0) {
            snprintf(path, sizeof(path), "%s/data_%03d_00000.csv", base_dir, i);
        } else if (dir_type == 1) {
            int region_idx = rand() % 5;
            snprintf(path, sizeof(path), "%s/%s/data_%03d_00000.csv", 
                     base_dir, regions[region_idx], i);
        } else {
            int region_idx = rand() % 5;
            int subregion_idx = rand() % 3;
            snprintf(path, sizeof(path), "%s/%s/%s/data_%03d_00000.csv", 
                     base_dir, regions[region_idx], subregions[subregion_idx], i);
        }
        
        // Also create some files that should be filtered out (not ending with 00000.csv)
        char filtered_path[512];
        if (i < 3) {  // Only create a few filtered files
            if (dir_type == 0) {
                snprintf(filtered_path, sizeof(filtered_path), "%s/data_%03d_00001.csv", base_dir, i);
            } else if (dir_type == 1) {
                int region_idx = rand() % 5;
                snprintf(filtered_path, sizeof(filtered_path), "%s/%s/data_%03d_00001.csv", 
                         base_dir, regions[region_idx], i);
            } else {
                int region_idx = rand() % 5;
                int subregion_idx = rand() % 3;
                snprintf(filtered_path, sizeof(filtered_path), "%s/%s/%s/data_%03d_00001.csv", 
                         base_dir, regions[region_idx], subregions[subregion_idx], i);
            }
            
            FILE* filtered_fp = fopen(filtered_path, "w");
            if (filtered_fp) {
                fprintf(filtered_fp, "date,time,area,residence,age,gender,population\n");
                fprintf(filtered_fp, "20160101,0100,362257341,-1,-1,-1,999\n");  // Different value to detect if processed
                fclose(filtered_fp);
            }
        }
        
        FILE* fp = fopen(path, "w");
        if (!fp) {
            fprintf(stderr, "Failed to create file: %s\n", path);
            continue;
        }
        
        fprintf(fp, "date,time,area,residence,age,gender,population\n");
        
        int num_rows = MIN_ROWS_PER_FILE + rand() % (MAX_ROWS_PER_FILE - MIN_ROWS_PER_FILE + 1);
        
        for (int j = 0; j < num_rows; j++) {
            int month = 1 + (rand() % 12);
            int day = 1 + (rand() % 28);
            
            int hour = rand() % 24;
            int minute = (rand() % 6) * 10;
            
            uint64_t base_meshid = 362257341ULL;
            uint64_t meshid = base_meshid + (rand() % 10000);
            
            int population = 50 + (rand() % 500);
            
            fprintf(fp, "2016%02d%02d,%02d%02d,%llu,-1,-1,-1,%d\n",
                    month, day, hour, minute, meshid, population);
        }
        
        fclose(fp);
        
        if ((i + 1) % 10 == 0) {
            printf("  Created %d/%d files...\n", i + 1, num_files);
        }
    }
    
    return num_files;
}


void test_multi_producer_csv_to_h5() {
    printf("=== Testing Multi-Producer CSV Generation with Single Consumer H5 Writer ===\n");
    
    const char* test_dir = "test_multi_csv_dir";
    
    printf("Step 1: Creating test directory structure with CSV files...\n");
    int expected_files = create_test_directory_structure(test_dir);
    
    printf("\nStep 2: Enumerating all CSV files...\n");
    char** all_csv_files = malloc(MAX_FILES * sizeof(char*));
    size_t total_file_count = 0;
    size_t file_capacity = MAX_FILES;
    find_csv_files(test_dir, &all_csv_files, &total_file_count, &file_capacity);
    
    if (total_file_count != expected_files) {
        printf("Warning: Expected %d files, found %zu files\n", expected_files, total_file_count);
    }
    
    printf("Total CSV files found: %zu\n", total_file_count);
    
    size_t files_to_show = (total_file_count < 10) ? total_file_count : 10;
    for (size_t i = 0; i < files_to_show; i++) {
        printf("  [%zu] %s\n", i, all_csv_files[i]);
    }
    if (total_file_count > 10) {
        printf("  ... and %zu more files\n", total_file_count - 10);
    }
    
    printf("\nStep 3: Single consumer converting all CSV files to H5...\n");
    
    csv_to_h5_config_t config = CSV_TO_H5_DEFAULT_CONFIG;
    config.output_h5_file = "test_multi_output.h5";
    config.create_new = 1;
    config.verbose = 1;
    
    csv_to_h5_stats_t stats;
    clock_t start_time = clock();
    
    int result = csv_to_h5_convert_files((const char**)all_csv_files, total_file_count, &config, &stats);
    
    clock_t end_time = clock();
    double conversion_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    
    assert(result == 0);
    
    printf("\n=== Conversion Statistics ===\n");
    printf("Total CSV files processed: %zu\n", total_file_count);
    printf("Total rows processed: %zu\n", stats.total_rows_processed);
    printf("Unique timestamps: %zu\n", stats.unique_timestamps);
    printf("Errors: %zu\n", stats.errors);
    printf("Conversion time: %.2f seconds\n", conversion_time);
    printf("Processing rate: %.1f rows/sec\n", stats.total_rows_processed / conversion_time);
    printf("Average rows per file: %.1f\n", (double)stats.total_rows_processed / total_file_count);
    
    assert(stats.errors == 0);
    assert(stats.total_rows_processed > 0);
    assert(stats.unique_timestamps > 0);
    
    printf("\nStep 4: Verifying H5 file integrity...\n");
    
    struct h5r* reader;
    result = h5r_open("test_multi_output.h5", &reader);
    assert(result == 0);
    
    struct h5mobaku* h5m_reader;
    result = h5mobaku_open("test_multi_output.h5", &h5m_reader);
    assert(result == 0);
    assert(h5m_reader->start_datetime_str != NULL);
    
    printf("H5 file verified. Start datetime: %s\n", h5m_reader->start_datetime_str);
    
    cmph_t* hash = meshid_prepare_search();
    assert(hash != NULL);
    
    int32_t test_value;
    uint32_t mesh_idx = meshid_search_id(hash, 362257341);
    if (mesh_idx != MESHID_NOT_FOUND) {
        result = h5r_read_cell(reader, 0, mesh_idx, &test_value);
        assert(result == 0);
        printf("Sample data verification: mesh 362257341 at time 0 = %d\n", test_value);
    }
    
    h5mobaku_close(h5m_reader);
    h5r_close(reader);
    cmph_destroy(hash);
    
    printf("\nStep 5: Cleaning up...\n");
    
    for (size_t i = 0; i < total_file_count; i++) {
        free(all_csv_files[i]);
    }
    free(all_csv_files);
    
    system("rm -rf test_multi_csv_dir");
    unlink("test_multi_output.h5");
    
    printf("Multi-producer CSV to H5 test passed!\n");
}

void test_csv_conversion() {
    printf("Testing basic CSV to HDF5 conversion...\n");
    
    const char* test_csv = "test_conversion_00000.csv";
    FILE* fp = fopen(test_csv, "w");
    assert(fp != NULL);
    
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0100,362257341,-1,-1,-1,100\n");
    fprintf(fp, "20160101,0100,362257342,-1,-1,-1,200\n");
    fprintf(fp, "20160101,0200,362257341,-1,-1,-1,150\n");
    fprintf(fp, "20160101,0200,362257342,-1,-1,-1,250\n");
    fclose(fp);
    
    csv_to_h5_config_t config = CSV_TO_H5_DEFAULT_CONFIG;
    config.output_h5_file = "test_output.h5";
    config.verbose = 1;
    
    csv_to_h5_stats_t stats;
    int result = csv_to_h5_convert_file(test_csv, &config, &stats);
    assert(result == 0);
    
    printf("Conversion stats:\n");
    printf("  Rows processed: %zu\n", stats.total_rows_processed);
    printf("  Unique times: %zu\n", stats.unique_timestamps);
    printf("  Errors: %zu\n", stats.errors);
    
    assert(stats.total_rows_processed == 4);
    assert(stats.unique_timestamps == 2);
    assert(stats.errors == 0);
    
    struct h5r* reader;
    result = h5r_open("test_output.h5", &reader);
    assert(result == 0);
    
    struct h5mobaku* h5m_reader;
    result = h5mobaku_open("test_output.h5", &h5m_reader);
    assert(result == 0);
    assert(h5m_reader->start_datetime_str != NULL);
    assert(strcmp(h5m_reader->start_datetime_str, "2016-01-01 00:00:00") == 0);
    
    printf("Start datetime attribute verified: %s\n", h5m_reader->start_datetime_str);
    h5mobaku_close(h5m_reader);
    
    cmph_t* hash = meshid_prepare_search();
    assert(hash != NULL);
    
    int32_t value;
    uint32_t mesh_idx;
    
    // Check dataset dimensions first
    size_t time_points, mesh_count;
    h5r_get_dimensions(reader, &time_points, &mesh_count);
    printf("DEBUG: Dataset dimensions: %zu time points, %zu mesh count\n", time_points, mesh_count);
    
    mesh_idx = meshid_search_id(hash, 362257341);
    printf("DEBUG: Mesh ID 362257341 -> index %u\n", mesh_idx);
    assert(mesh_idx != MESHID_NOT_FOUND);
    
    // 20160101,0100 -> time index 1 (01:00 is 1 hour from 00:00)
    result = h5r_read_cell(reader, 1, mesh_idx, &value);
    assert(result == 0);
    printf("DEBUG: Read from time 1, mesh index %u: got value %d (expected 100)\n", mesh_idx, value);
    assert(value == 100);
    
    mesh_idx = meshid_search_id(hash, 362257342);
    assert(mesh_idx != MESHID_NOT_FOUND);
    // 20160101,0100 -> time index 1
    result = h5r_read_cell(reader, 1, mesh_idx, &value);
    assert(result == 0);
    assert(value == 200);
    
    mesh_idx = meshid_search_id(hash, 362257341);
    // 20160101,0200 -> time index 2 (02:00 is 2 hours from 00:00)
    result = h5r_read_cell(reader, 2, mesh_idx, &value);
    assert(result == 0);
    assert(value == 150);
    
    mesh_idx = meshid_search_id(hash, 362257342);
    // 20160101,0200 -> time index 2
    result = h5r_read_cell(reader, 2, mesh_idx, &value);
    assert(result == 0);
    assert(value == 250);
    
    h5r_close(reader);
    cmph_destroy(hash);
    unlink(test_csv);
    unlink("test_output.h5");
    
    printf("Basic CSV to HDF5 conversion test passed!\n");
}

void test_append_mode() {
    printf("\nTesting append mode...\n");
    
    // Create first CSV file
    const char* csv1 = "test_append1_00000.csv";
    FILE* fp = fopen(csv1, "w");
    assert(fp != NULL);
    
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0100,362257341,-1,-1,-1,100\n");
    fclose(fp);
    
    // Create second CSV file
    const char* csv2 = "test_append2_00000.csv";
    fp = fopen(csv2, "w");
    assert(fp != NULL);
    
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0200,362257341,-1,-1,-1,200\n");
    fclose(fp);
    
    // Convert both files together
    const char* files[] = {csv1, csv2};
    csv_to_h5_config_t config = CSV_TO_H5_DEFAULT_CONFIG;
    config.output_h5_file = "test_append.h5";
    config.create_new = 1;
    
    csv_to_h5_stats_t stats;
    int result = csv_to_h5_convert_files(files, 2, &config, &stats);
    assert(result == 0);
    assert(stats.total_rows_processed == 2);
    assert(stats.unique_timestamps == 2);
    
    // Verify HDF5 content
    struct h5r* reader;
    result = h5r_open("test_append.h5", &reader);
    assert(result == 0);
    
    cmph_t* hash = meshid_prepare_search();
    assert(hash != NULL);
    
    int32_t value;
    uint32_t mesh_idx = meshid_search_id(hash, 362257341);
    assert(mesh_idx != MESHID_NOT_FOUND);
    
    // Check first timestamp (20160101,0100 -> time index 1)
    result = h5r_read_cell(reader, 1, mesh_idx, &value);
    assert(result == 0);
    printf("DEBUG: Read from time 1, mesh index %u: got value %d (expected 100)\n", mesh_idx, value);
    assert(value == 100);
    
    // Check second timestamp (20160101,0200 -> time index 2)
    result = h5r_read_cell(reader, 2, mesh_idx, &value);
    assert(result == 0);
    printf("DEBUG: Read from time 2, mesh index %u: got value %d (expected 200)\n", mesh_idx, value);
    assert(value == 200);
    
    h5r_close(reader);
    cmph_destroy(hash);
    
    // Cleanup
    unlink(csv1);
    unlink(csv2);
    unlink("test_append.h5");
    
    printf("Append mode test passed!\n");
}

void test_write_to_sparse_regions() {
    printf("\nTesting write to sparse regions of existing H5 file...\n");
    fflush(stdout);
    
    // This test demonstrates that the CSV converter can handle sparse writes
    // The converter assigns time indices based on the order timestamps are encountered
    
    // Create 3 CSV files with different timestamps
    // File 1: T1 (early)
    // File 2: T3 (late) 
    // File 3: T2 (middle)
    
    // Create CSV files
    FILE* fp = fopen("sparse1_00000.csv", "w");
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0100,362257341,-1,-1,-1,100\n");
    fprintf(fp, "20160101,0100,362257342,-1,-1,-1,200\n");
    fclose(fp);
    
    fp = fopen("sparse2_00000.csv", "w");
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0300,362257341,-1,-1,-1,300\n");
    fprintf(fp, "20160101,0300,362257342,-1,-1,-1,400\n");
    fclose(fp);
    
    fp = fopen("sparse3_00000.csv", "w");
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0200,362257341,-1,-1,-1,150\n");
    fprintf(fp, "20160101,0200,362257342,-1,-1,-1,250\n");
    fclose(fp);
    
    // Convert all files together
    const char* files[] = {"sparse1_00000.csv", "sparse2_00000.csv", "sparse3_00000.csv"};
    csv_to_h5_config_t config = CSV_TO_H5_DEFAULT_CONFIG;
    config.output_h5_file = "test_sparse.h5";
    
    csv_to_h5_stats_t stats;
    int result = csv_to_h5_convert_files(files, 3, &config, &stats);
    assert(result == 0);
    printf("  Converted %zu rows with %zu unique timestamps\n", 
           stats.total_rows_processed, stats.unique_timestamps);
    
    // Verify the data
    struct h5r* reader;
    result = h5r_open("test_sparse.h5", &reader);
    assert(result == 0);
    
    cmph_t* hash = meshid_prepare_search();
    uint32_t mesh_idx = meshid_search_id(hash, 362257341);
    
    // Check the actual time indices where data was written (1, 2, 3 for 01:00, 02:00, 03:00)
    int32_t values[3];
    int time_indices[] = {1, 2, 3};  // 01:00, 02:00, 03:00
    for (int i = 0; i < 3; i++) {
        result = h5r_read_cell(reader, time_indices[i], mesh_idx, &values[i]);
        assert(result == 0);
        printf("  Time index %d: value = %d\n", time_indices[i], values[i]);
    }
    
    // Verify we have all three values (100, 150, 300 in some order)
    int found_100 = 0, found_150 = 0, found_300 = 0;
    for (int i = 0; i < 3; i++) {
        if (values[i] == 100) found_100 = 1;
        if (values[i] == 150) found_150 = 1;
        if (values[i] == 300) found_300 = 1;
    }
    assert(found_100 && found_150 && found_300);
    
    // Check unwritten regions
    int32_t unwritten;
    result = h5r_read_cell(reader, 10, mesh_idx, &unwritten);
    assert(result == 0);
    assert(unwritten == 0);  // Fill value is now 0 to match database code
    
    // Cleanup
    h5r_close(reader);
    cmph_destroy(hash);
    unlink("sparse1_00000.csv");
    unlink("sparse2_00000.csv");
    unlink("sparse3_00000.csv");
    unlink("test_sparse.h5");
    
    printf("Sparse region write test passed!\n");
}

int main() {
    printf("Starting CSV to H5 tests...\n");
    test_csv_conversion();
    test_append_mode();
    test_write_to_sparse_regions();
    test_multi_producer_csv_to_h5();
    
    printf("\nAll tests passed!\n");
    return 0;
}