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

// Measure time for looping single-mesh access over the given meshes and time range
double measure_single_access_loop(struct h5r *h5_ctx, cmph_t *hash,
                                  const uint32_t *mesh_ids, size_t mesh_count,
                                  int start_time, int end_time) {
    clock_t start = clock();
    for (size_t i = 0; i < mesh_count; i++) {
        for (int t = start_time; t <= end_time; t++) {
            volatile int32_t pop = h5mobaku_read_population_single(h5_ctx, hash, mesh_ids[i], t);
            (void)pop;
        }
    }
    clock_t end = clock();
    return ((double)(end - start)) / CLOCKS_PER_SEC;
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
    // Calculate 2 years with leap year consideration: 2016 (leap) + 2017 (non-leap) = 366 + 365 = 731 days
    int end_time = (366 + 365) * 24 - 1; // 17543 hours for 2016-2017 period
    
    printf("Reading 2 years of data (%d hours) for mesh ID: %u\n", end_time - start_time + 1, mesh_id);
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
    double single_access_time = 0;

    start = clock();
    volatile int32_t pop = h5mobaku_read_population_single(h5_ctx, hash, test_mesh, 1000);
    end = clock();
    single_access_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("  Average time per access: %.6f seconds\n", single_access_time);
    
    // Test batch access performance
    printf("\n2. Batch access performance:\n");
    uint32_t batch_meshes[] = {574036194, 574036192, 533925251, 574036191, 574036201};
    size_t batch_size = sizeof(batch_meshes) / sizeof(batch_meshes[0]);
    double batch_single_loop_time = measure_single_access_loop(h5_ctx, hash, batch_meshes, batch_size, 1000, 1000);
    printf("  Single access loop for %zu meshes (same condition): %.6f seconds\n",
           batch_size, batch_single_loop_time);
    
    start = clock();
    volatile int32_t *batch_result = h5mobaku_read_population_multi(h5_ctx, hash, batch_meshes, batch_size, 1000);
    end = clock();
    double batch_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    
    if (batch_result) {
        printf("  Time for %zu meshes: %.6f seconds\n", 
               batch_size, batch_time);
        if (batch_time > 0) {
            printf("  Speed vs single access loop: %.2fx faster\n", batch_single_loop_time / batch_time);
        }
        h5mobaku_free_data((int32_t*)batch_result);
    }
    
    // Test time series performance
    printf("\n3. Time series access performance:\n");
    double ts_single_loop_time = measure_single_access_loop(h5_ctx, hash, &test_mesh, 1, 0, 9999);
    printf("  Single access loop for 10000 hours: %.6f seconds\n", ts_single_loop_time);

    start = clock();
    int32_t *ts = h5mobaku_read_population_time_series(h5_ctx, hash, test_mesh, 0, 9999);
    end = clock();
    double time_series_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    
    if (ts) {
        printf("  Time for 10000 hours: %.6f seconds\n",
               time_series_time);
        if (time_series_time > 0) {
            printf("  Speed vs single access loop: %.2fx faster\n", ts_single_loop_time / time_series_time);
        }
        h5mobaku_free_data(ts);
    }
    
    // Test new optimized multi-mesh multi-time series function
    printf("\n4. Multi-mesh multi-time series performance:\n");
    uint32_t multi_meshes[] = {
    574036324, 574036333, 574036334, 574036343, 574036344, 574036353, 574036354, 574036363, 574036364, 574036373, 574036374, 574036383,
    574036384, 574036393, 574036394, 574037303, 574037304, 574037313, 574036322, 574036331, 574036332, 574036341, 574036342, 574036351,
    574036352, 574036361, 574036362, 574036371, 574036372, 574036381, 574036382, 574036391, 574036392, 574037301, 574037302, 574037311,
    574036224, 574036233, 574036234, 574036243, 574036244, 574036253, 574036254, 574036263, 574036264, 574036273, 574036274, 574036283,
    574036284, 574036293, 574036294, 574037203, 574037204, 574037213, 574036222, 574036231, 574036232, 574036241, 574036242, 574036251,
    574036252, 574036261, 574036262, 574036271, 574036272, 574036281, 574036282, 574036291, 574036292, 574037201, 574037202, 574037211,
    574036124, 574036133, 574036134, 574036143, 574036144, 574036153, 574036154, 574036163, 574036164, 574036173, 574036174, 574036183,
    574036184, 574036193, 574036194, 574037103, 574037104, 574037113, 574036122, 574036131, 574036132, 574036141, 574036142, 574036151,
    574036152, 574036161, 574036162, 574036171, 574036172, 574036181, 574036182, 574036191, 574036192, 574037101, 574037102, 574037111,
    574036024, 574036033, 574036034, 574036043, 574036044, 574036053, 574036054, 574036063, 574036064, 574036073, 574036074, 574036083,
    574036084, 574036093, 574036094, 574037003, 574037004, 574037013, 574036022, 574036031, 574036032, 574036041, 574036042, 574036051,
    574036052, 574036061, 574036062, 574036071, 574036072, 574036081, 574036082, 574036091, 574036092, 574037001, 574037002, 574037011,
    574026924, 574026933, 574026934, 574026943, 574026944, 574026953, 574026954, 574026963, 574026964, 574026973, 574026974, 574026983,
    574026984, 574026993, 574026994, 574027903, 574027904, 574027913, 574026922, 574026931, 574026932, 574026941, 574026942, 574026951,
    574026952, 574026961, 574026962, 574026971, 574026972, 574026981, 574026982, 574026991, 574026992, 574027901, 574027902, 574027911,
    574026824, 574026833, 574026834, 574026843, 574026844, 574026853, 574026854, 574026863, 574026864, 574026873, 574026874, 574026883,
    574026884, 574026893, 574026894, 574027803, 574027804, 574027813, 574026822, 574026831, 574026832, 574026841, 574026842, 574026851,
    574026852, 574026861, 574026862, 574026871, 574026872, 574026881, 574026882, 574026891, 574026892, 574027801, 574027802, 574027811,
    574026724, 574026733, 574026734, 574026743, 574026744, 574026753, 574026754, 574026763, 574026764, 574026773, 574026774, 574026783,
    574026784, 574026793, 574026794, 574027703, 574027704, 574027713, 574026722, 574026731, 574026732, 574026741, 574026742, 574026751,
    574026752, 574026761, 574026762, 574026771, 574026772, 574026781, 574026782, 574026791, 574026792, 574027701, 574027702, 574027711,
    574026624, 574026633, 574026634, 574026643, 574026644, 574026653, 574026654, 574026663, 574026664, 574026673, 574026674, 574026683,
    574026684, 574026693, 574026694, 574027603, 574027604, 574027613, 574026622, 574026631, 574026632, 574026641, 574026642, 574026651,
    574026652, 574026661, 574026662, 574026671, 574026672, 574026681, 574026682, 574026691, 574026692, 574027601, 574027602, 574027611,
    574026524, 574026533, 574026534, 574026543, 574026544, 574026553, 574026554, 574026563, 574026564, 574026573, 574026574, 574026583,
    574026584, 574026593, 574026594, 574027503, 574027504, 574027513, 574026522, 574026531, 574026532, 574026541, 574026542, 574026551,
    574026552, 574026561, 574026562, 574026571, 574026572, 574026581, 574026582, 574026591, 574026592, 574027501, 574027502, 574027511
};
    size_t num_meshes = sizeof(multi_meshes) / sizeof(multi_meshes[0]);
    int start_time = 0;
    int end_time = 340;
    double multi_single_loop_time = measure_single_access_loop(h5_ctx, hash, multi_meshes, num_meshes, start_time, end_time);
    
    printf("  Reading %zu meshes × %d hours = %zu values\n", 
           num_meshes, end_time - start_time + 1, num_meshes * (end_time - start_time + 1));
    printf("  Single access loop for same range: %.6f seconds\n", multi_single_loop_time);
    
    start = clock();
    int32_t *multi_ts = h5mobaku_read_multi_mesh_time_series(h5_ctx, hash, multi_meshes, num_meshes, start_time, end_time);
    end = clock();
    double multi_ts_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    
    if (multi_ts) {
        printf("  Time for multi-mesh multi-time series: %.6f seconds\n", multi_ts_time);
        printf("  Values per second: %.0f\n", (num_meshes * 1000.0) / multi_ts_time);
        if (multi_ts_time > 0) {
            printf("  Speed vs single access loop: %.2fx faster\n", multi_single_loop_time / multi_ts_time);
        }
        
        // Show sample values (first time point for each mesh)
        printf("  Sample values (t=0):");
        for (size_t i = 0; i < num_meshes; i++) {
            printf(" %d", multi_ts[i]);
        }
        printf("\n");
        
        h5mobaku_free_data(multi_ts);
    }
    
    // Compare traditional approach vs optimized approach
    printf("\n5. Comparison: Traditional vs Optimized for %zu meshes × %d hours:\n", num_meshes, end_time);
    double single_loop_compare = measure_single_access_loop(h5_ctx, hash, multi_meshes, num_meshes, 0, end_time);
    printf("  Single access loop (%zu meshes × 1000 hours): %.6f seconds\n", num_meshes, single_loop_compare);
    
    // Traditional approach: multiple calls
    start = clock();
    for (size_t i = 0; i < num_meshes; i++) {
        int32_t *single_ts = h5mobaku_read_population_time_series(h5_ctx, hash, multi_meshes[i], 0, end_time);
        if (single_ts) h5mobaku_free_data(single_ts);
    }
    end = clock();
    double traditional_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("  Traditional (multiple calls): %.6f seconds\n", traditional_time);
    if (traditional_time > 0) {
        printf("  Speed vs single access loop: %.2fx faster\n", single_loop_compare / traditional_time);
    }
    
    // Optimized approach: single call
    start = clock();
    int32_t *opt_result = h5mobaku_read_multi_mesh_time_series(h5_ctx, hash, multi_meshes, num_meshes, 0, end_time);
    end = clock();
    double optimized_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    if (opt_result) {
        printf("  Optimized (single call): %.6f seconds\n", optimized_time);
        printf("  Speedup: %.2fx faster\n", traditional_time / optimized_time);
        if (optimized_time > 0) {
            printf("  Speed vs single access loop: %.2fx faster\n", single_loop_compare / optimized_time);
        }
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
