//
// Test for CSV to HDF5 conversion
//

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "csv_to_h5_converter.h"
#include "H5MR/h5mr.h"
#include "h5mobaku_ops.h"
#include "meshid_ops.h"

void test_csv_conversion() {
    printf("Testing CSV to HDF5 conversion...\n");
    
    // Create test CSV file in current directory
    const char* test_csv = "test_conversion.csv";
    FILE* fp = fopen(test_csv, "w");
    assert(fp != NULL);
    
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20240101,0100,362257341,-1,-1,-1,100\n");
    fprintf(fp, "20240101,0100,362257342,-1,-1,-1,200\n");
    fprintf(fp, "20240101,0200,362257341,-1,-1,-1,150\n");
    fprintf(fp, "20240101,0200,362257342,-1,-1,-1,250\n");
    fclose(fp);
    
    // Convert to HDF5
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
    
    // Verify HDF5 content
    struct h5r* reader;
    result = h5r_open("test_output.h5", &reader);
    assert(result == 0);
    
    // Verify start_datetime attribute using h5mobaku wrapper
    struct h5mobaku* h5m_reader;
    result = h5mobaku_open("test_output.h5", &h5m_reader);
    assert(result == 0);
    assert(h5m_reader->start_datetime_str != NULL);
    assert(strcmp(h5m_reader->start_datetime_str, "2016-01-01 00:00:00") == 0);
    
    printf("Start datetime attribute verified: %s\n", h5m_reader->start_datetime_str);
    h5mobaku_close(h5m_reader);
    
    // Prepare mesh search
    cmph_t* hash = meshid_prepare_search();
    assert(hash != NULL);
    
    // Check values
    int32_t value;
    uint32_t mesh_idx;
    
    // Check first timestamp, first mesh
    mesh_idx = meshid_search_id(hash, 362257341);
    assert(mesh_idx != MESHID_NOT_FOUND);
    result = h5r_read_cell(reader, 0, mesh_idx, &value);
    assert(result == 0);
    assert(value == 100);
    
    // Check first timestamp, second mesh
    mesh_idx = meshid_search_id(hash, 362257342);
    assert(mesh_idx != MESHID_NOT_FOUND);
    result = h5r_read_cell(reader, 0, mesh_idx, &value);
    assert(result == 0);
    assert(value == 200);
    
    // Check second timestamp, first mesh
    mesh_idx = meshid_search_id(hash, 362257341);
    result = h5r_read_cell(reader, 1, mesh_idx, &value);
    assert(result == 0);
    assert(value == 150);
    
    // Check second timestamp, second mesh
    mesh_idx = meshid_search_id(hash, 362257342);
    result = h5r_read_cell(reader, 1, mesh_idx, &value);
    assert(result == 0);
    assert(value == 250);
    
    // Cleanup
    h5r_close(reader);
    cmph_destroy(hash);
    unlink(test_csv);
    unlink("test_output.h5");
    
    printf("CSV to HDF5 conversion test passed!\n");
}

void test_append_mode() {
    printf("\nTesting append mode...\n");
    
    // Create first CSV file
    const char* csv1 = "test_append1.csv";
    FILE* fp = fopen(csv1, "w");
    assert(fp != NULL);
    
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20240101,0100,362257341,-1,-1,-1,100\n");
    fclose(fp);
    
    // Create second CSV file
    const char* csv2 = "test_append2.csv";
    fp = fopen(csv2, "w");
    assert(fp != NULL);
    
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20240101,0200,362257341,-1,-1,-1,200\n");
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
    
    // Check first timestamp
    result = h5r_read_cell(reader, 0, mesh_idx, &value);
    assert(result == 0);
    assert(value == 100);
    
    // Check second timestamp
    result = h5r_read_cell(reader, 1, mesh_idx, &value);
    assert(result == 0);
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
    FILE* fp = fopen("sparse1.csv", "w");
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0100,362257341,-1,-1,-1,100\n");
    fprintf(fp, "20160101,0100,362257342,-1,-1,-1,200\n");
    fclose(fp);
    
    fp = fopen("sparse2.csv", "w");
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0300,362257341,-1,-1,-1,300\n");
    fprintf(fp, "20160101,0300,362257342,-1,-1,-1,400\n");
    fclose(fp);
    
    fp = fopen("sparse3.csv", "w");
    fprintf(fp, "date,time,area,residence,age,gender,population\n");
    fprintf(fp, "20160101,0200,362257341,-1,-1,-1,150\n");
    fprintf(fp, "20160101,0200,362257342,-1,-1,-1,250\n");
    fclose(fp);
    
    // Convert all files together
    const char* files[] = {"sparse1.csv", "sparse2.csv", "sparse3.csv"};
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
    
    // Check that all values are stored
    int32_t values[3];
    for (int i = 0; i < 3; i++) {
        result = h5r_read_cell(reader, i, mesh_idx, &values[i]);
        assert(result == 0);
        printf("  Time index %d: value = %d\n", i, values[i]);
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
    unlink("sparse1.csv");
    unlink("sparse2.csv");
    unlink("sparse3.csv");
    unlink("test_sparse.h5");
    
    printf("Sparse region write test passed!\n");
}

int main() {
    test_csv_conversion();
    test_append_mode();
    test_write_to_sparse_regions();
    
    printf("\nAll tests passed!\n");
    return 0;
}