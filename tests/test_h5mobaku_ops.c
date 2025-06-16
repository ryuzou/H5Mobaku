//
// Created by ryuzot on 25/05/31.
//

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include "h5mobaku_ops.h"
#include "H5MR/h5mr.h"
#include "meshid_ops.h"
#include "env_utils.h"

// Helper function to get test file path
const char* get_test_file_path() {
    const char* path = get_env_value("HDF5_FILE_PATH", NULL);
    if (!path) {
        fprintf(stderr, "Error: HDF5_FILE_PATH not set in environment or .env file\n");
        exit(1);
    }
    return path;
}

// Helper function to print test results
void print_test_result(const char *test_name, int passed) {
    printf("[%s] %s\n", passed ? "PASS" : "FAIL", test_name);
}

// Forward declaration
void test_datetime_based_api(cmph_t *hash);

// Test single mesh population reading
void test_single_mesh_read(struct h5r *h5_ctx, cmph_t *hash) {
    printf("\n=== Testing Single Mesh Read ===\n");
    
    // Test mesh IDs (examples from Python code)
    uint32_t test_meshes[] = {574036191, 574036192, 533925251};
    int test_time_index = 1000; // Arbitrary time index
    
    for (int i = 0; i < 3; i++) {
        int32_t population = h5mobaku_read_population_single(h5_ctx, hash, test_meshes[i], test_time_index);
        
        printf("Mesh ID: %u, Time Index: %d, Population: %d\n", 
               test_meshes[i], test_time_index, population);
        
        print_test_result("Single mesh read", population >= 0);
    }
}

// Test multiple mesh population reading
void test_multi_mesh_read(struct h5r *h5_ctx, cmph_t *hash) {
    printf("\n=== Testing Multiple Mesh Read ===\n");
    
    uint32_t mesh_ids[] = {574036191, 574036192, 533925251, 574036193};
    size_t num_meshes = sizeof(mesh_ids) / sizeof(mesh_ids[0]);
    int test_time_index = 2000;
    
    int32_t *populations = h5mobaku_read_population_multi(h5_ctx, hash, mesh_ids, num_meshes, test_time_index);
    
    if (populations) {
        printf("Time Index: %d\n", test_time_index);
        for (size_t i = 0; i < num_meshes; i++) {
            printf("  Mesh ID: %u, Population: %d\n", mesh_ids[i], populations[i]);
        }
        h5mobaku_free_data(populations);
        print_test_result("Multi mesh read", 1);
    } else {
        print_test_result("Multi mesh read", 0);
    }
}

// Test time series reading
void test_time_series_read(struct h5r *h5_ctx, cmph_t *hash) {
    printf("\n=== Testing Time Series Read ===\n");
    
    uint32_t mesh_id = 574036191;
    int start_time = 0;
    int end_time = 17519; // 2 years = 365 * 2 * 24 - 1 hours
    
    printf("Reading 2 years of data (17,520 hours) for mesh ID: %u\n", mesh_id);
    clock_t start_clock = clock();
    
    int32_t *time_series = h5mobaku_read_population_time_series(h5_ctx, hash, mesh_id, start_time, end_time);
    
    clock_t end_clock = clock();
    double elapsed_time = ((double)(end_clock - start_clock)) / CLOCKS_PER_SEC;
    
    if (time_series) {
        printf("Successfully read 2 years of data in %.6f seconds\n", elapsed_time);
        
        // Display first 5 values
        printf("\nFirst 5 hours:\n");
        for (int i = 0; i < 5; i++) {
            char *datetime_str = meshid_get_datetime_from_time_index(start_time + i);
            printf("  Hour %d - %s: %d\n", i, datetime_str ? datetime_str : "Unknown", time_series[i]);
            if (datetime_str) free(datetime_str);
        }
        
        // Display last 5 values
        printf("\nLast 5 hours:\n");
        int total_hours = end_time - start_time + 1;
        for (int i = total_hours - 5; i < total_hours; i++) {
            char *datetime_str = meshid_get_datetime_from_time_index(start_time + i);
            printf("  Hour %d - %s: %d\n", i, datetime_str ? datetime_str : "Unknown", time_series[i]);
            if (datetime_str) free(datetime_str);
        }
        
        h5mobaku_free_data(time_series);
        print_test_result("Time series read (2 years)", 1);
    } else {
        print_test_result("Time series read (2 years)", 0);
    }
}


// Performance test similar to Python version
void test_performance(struct h5r *h5_ctx, cmph_t *hash) {
    printf("\n=== Performance Testing ===\n");
    
    // Test single mesh access performance
    printf("\n1. Single mesh access performance:\n");
    uint32_t test_mesh = 574036191;
    clock_t start, end;
    double total_time = 0;

    start = clock();
    volatile int32_t pop = h5mobaku_read_population_single(h5_ctx, hash, test_mesh, 1000);
    end = clock();
    total_time += ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("  Average time per access: %.6f seconds\n", total_time);
    
    // Test batch access performance
    printf("\n2. Batch access performance:\n");
    uint32_t batch_meshes[] = {574036194, 574036192, 533925251, 574036191, 574036201};
    size_t batch_size = sizeof(batch_meshes) / sizeof(batch_meshes[0]);
    
    start = clock();
    volatile int32_t *batch_result = h5mobaku_read_population_multi(h5_ctx, hash, batch_meshes, batch_size, 1000);
    end = clock();
    
    if (batch_result) {
        printf("  Time for %zu meshes: %.6f seconds\n", 
               batch_size, ((double)(end - start)) / CLOCKS_PER_SEC);
        h5mobaku_free_data((int32_t*)batch_result);
    }
    
    // Test time series performance
    printf("\n3. Time series access performance:\n");
    start = clock();
    int32_t *ts = h5mobaku_read_population_time_series(h5_ctx, hash, test_mesh, 0, 9999);
    end = clock();
    
    if (ts) {
        printf("  Time for 10000 hours: %.6f seconds\n",
               ((double)(end - start)) / CLOCKS_PER_SEC);
        h5mobaku_free_data(ts);
    }
    
    // Test new optimized multi-mesh multi-time series function
    printf("\n4. Multi-mesh multi-time series performance:\n");
    uint32_t multi_meshes[] = {574036191, 574036192, 533925251, 574036193, 574036194, 362257264, 362257272, 684827002,684827003,684827004,684827101,684827102,684827103,684827104,684827201,684827202,684827203,684827204,684827301,684827302,684827303,684827304};
    size_t num_meshes = sizeof(multi_meshes) / sizeof(multi_meshes[0]);
    int start_time = 0;
    int end_time = 999; // 1000 hours
    
    printf("  Reading %zu meshes × %d hours = %zu values\n", 
           num_meshes, end_time - start_time + 1, num_meshes * (end_time - start_time + 1));
    
    start = clock();
    int32_t *multi_ts = h5mobaku_read_multi_mesh_time_series(h5_ctx, hash, multi_meshes, num_meshes, start_time, end_time);
    end = clock();
    
    if (multi_ts) {
        double elapsed = ((double)(end - start)) / CLOCKS_PER_SEC;
        printf("  Time for multi-mesh multi-time series: %.6f seconds\n", elapsed);
        printf("  Values per second: %.0f\n", (num_meshes * 1000.0) / elapsed);
        
        // Show sample values (first time point for each mesh)
        printf("  Sample values (t=0):");
        for (size_t i = 0; i < num_meshes; i++) {
            printf(" %d", multi_ts[i]);
        }
        printf("\n");
        
        h5mobaku_free_data(multi_ts);
    }
    
    // Compare traditional approach vs optimized approach
    printf("\n5. Comparison: Traditional vs Optimized for %zu meshes × 10000 hours:\n", num_meshes);
    
    // Traditional approach: multiple calls
    start = clock();
    for (size_t i = 0; i < num_meshes; i++) {
        int32_t *single_ts = h5mobaku_read_population_time_series(h5_ctx, hash, multi_meshes[i], 0, 9999);
        if (single_ts) h5mobaku_free_data(single_ts);
    }
    end = clock();
    double traditional_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("  Traditional (multiple calls): %.6f seconds\n", traditional_time);
    
    // Optimized approach: single call
    start = clock();
    int32_t *opt_result = h5mobaku_read_multi_mesh_time_series(h5_ctx, hash, multi_meshes, num_meshes, 0, 9999);
    end = clock();
    double optimized_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    if (opt_result) {
        printf("  Optimized (single call): %.6f seconds\n", optimized_time);
        printf("  Speedup: %.2fx faster\n", traditional_time / optimized_time);
        h5mobaku_free_data(opt_result);
    }
}

int main(int argc, char *argv[]) {
    printf("=== H5Mobaku Operations Test Suite ===\n");
    
    // Initialize CMPH hash
    cmph_t *hash = meshid_prepare_search();
    if (!hash) {
        fprintf(stderr, "Failed to initialize CMPH hash\n");
        return 1;
    }
    
    // Open HDF5 file
    const char* test_file = get_test_file_path();
    struct h5r *h5_ctx;
    int ret = h5r_open(test_file, &h5_ctx);
    if (ret < 0) {
        fprintf(stderr, "Failed to open HDF5 file: %s\n", test_file);
        cmph_destroy(hash);
        return 1;
    }
    
    printf("Successfully opened HDF5 file: %s\n", test_file);
    
    // Run tests
    test_single_mesh_read(h5_ctx, hash);
    test_multi_mesh_read(h5_ctx, hash);
    test_time_series_read(h5_ctx, hash);
    test_performance(h5_ctx, hash);
    test_datetime_based_api(hash);

    // Cleanup
    h5r_close(h5_ctx);
    cmph_destroy(hash);
    

    printf("\n=== All tests completed ===\n");
    return 0;
}

// Additional datetime-based tests
void test_datetime_based_api(cmph_t *hash) {
    printf("\n\n=== Testing Datetime-based API ===\n");
    
    // Open HDF5 file with h5mobaku wrapper
    const char* test_file = get_test_file_path();
    struct h5mobaku *ctx;
    int ret = h5mobaku_open(test_file, &ctx);
    if (ret < 0) {
        fprintf(stderr, "Failed to open HDF5 file with h5mobaku: %s\n", test_file);
        return;
    }
    
    printf("Successfully opened HDF5 file with h5mobaku\n");
    printf("Start datetime from HDF5: %s\n", ctx->start_datetime_str);
    
    // Test single mesh read at specific datetime
    printf("\n1. Testing single mesh read at specific datetime:\n");
    uint32_t test_mesh = 362257264;
    const char *test_datetime = "2024-01-01 01:00:00";
    int32_t population = h5mobaku_read_population_single_at_time(ctx, hash, test_mesh, test_datetime);
    printf("  Mesh ID: %u at %s, Population: %d\n", test_mesh, test_datetime, population);
    print_test_result("Datetime-based single mesh read", population >= 0);
    
    // Test multiple mesh read at specific datetime
    printf("\n2. Testing multiple mesh read at specific datetime:\n");
    uint32_t mesh_ids[] = {362257272, 574036191, 574036192, 574036193, 362257264, 574036194, 362257284};
    size_t num_meshes = sizeof(mesh_ids) / sizeof(mesh_ids[0]);
    const char *test_datetime2 = "2024-01-01 01:00:00";
    int32_t *populations = h5mobaku_read_population_multi_at_time(ctx, hash, mesh_ids, num_meshes, test_datetime2);
    
    if (populations) {
        printf("  At %s:\n", test_datetime2);
        for (size_t i = 0; i < num_meshes; i++) {
            printf("    Mesh ID: %u, Population: %d\n", mesh_ids[i], populations[i]);
        }
        h5mobaku_free_data(populations);
        print_test_result("Datetime-based multi mesh read", 1);
    } else {
        print_test_result("Datetime-based multi mesh read", 0);
    }
    
    // Test time series between two datetimes
    printf("\n3. Testing time series between two datetimes:\n");
    const char *start_dt = "2016-01-10 00:00:00";
    const char *end_dt = "2016-01-10 23:00:00";
    int32_t *time_series = h5mobaku_read_population_time_series_between(ctx, hash, test_mesh, start_dt, end_dt);
    
    if (time_series) {
        printf("  Mesh ID: %u from %s to %s\n", test_mesh, start_dt, end_dt);
        printf("  First 5 hours:\n");
        for (int i = 0; i < 5; i++) {
            printf("    Hour %d: %d\n", i, time_series[i]);
        }
        h5mobaku_free_data(time_series);
        print_test_result("Datetime-based time series", 1);
    } else {
        print_test_result("Datetime-based time series", 0);
    }
    
    
    // Cleanup
    h5mobaku_close(ctx);
    
    printf("\n=== Datetime-based API tests completed ===\n");
}