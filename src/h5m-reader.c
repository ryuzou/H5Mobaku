#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include "h5mobaku_ops.h"
#include "meshid_ops.h"

#define MAX_DATE_LEN 20

static void print_usage(const char *prog_name) {
    fprintf(stderr, "Usage: %s -f <hdf5_file> -m <mesh_id> [-t <datetime>] [-s <start_datetime> -e <end_datetime>] [-r]\n", prog_name);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  -f, --file <path>        HDF5 file path (required)\n");
    fprintf(stderr, "  -m, --mesh <id>          Mesh ID (required)\n");
    fprintf(stderr, "  -t, --time <datetime>    Single datetime (YYYY-MM-DD HH:MM:SS)\n");
    fprintf(stderr, "  -s, --start <datetime>   Start datetime for range query\n");
    fprintf(stderr, "  -e, --end <datetime>     End datetime for range query\n");
    fprintf(stderr, "  -r, --raw                Output raw uint32 byte stream (for piping; test by piping to 'od -An -t u4' for human)\n");
    fprintf(stderr, "  -h, --help               Show this help message\n");
    fprintf(stderr, "\nExamples:\n");
    fprintf(stderr, "  Single time query:\n");
    fprintf(stderr, "    %s -f data.h5 -m 533946395 -t \"2016-01-01 12:00:00\"\n", prog_name);
    fprintf(stderr, "  Time range query:\n");
    fprintf(stderr, "    %s -f data.h5 -m 533946395 -s \"2016-01-01 00:00:00\" -e \"2016-01-01 23:00:00\"\n", prog_name);
    fprintf(stderr, "  Raw output for piping:\n");
    fprintf(stderr, "    %s -f data.h5 -m 533946395 -s \"2016-01-01 00:00:00\" -e \"2016-01-01 01:00:00\" -r | external_analysis_program \n", prog_name);
}

static void print_table_header_single() {
    printf("\n");
    printf("+------------+---------------------+------------+\n");
    printf("| Mesh ID    | Datetime            | Population |\n");
    printf("+------------+---------------------+------------+\n");
}

static void print_table_row_single(uint32_t mesh_id, const char *datetime, int32_t population) {
    printf("| %-10u | %-19s | %10d |\n", mesh_id, datetime, population);
}

static void print_table_footer_single() {
    printf("+------------+---------------------+------------+\n");
}

static void print_table_header_range() {
    printf("\n");
    printf("+------------+---------------------+------------+\n");
    printf("| Mesh ID    | Datetime            | Population |\n");
    printf("+------------+---------------------+------------+\n");
}

static void print_table_row_range(uint32_t mesh_id, const char *datetime, int32_t population) {
    printf("| %-10u | %-19s | %10d |\n", mesh_id, datetime, population);
}

static void print_table_footer_range(int count) {
    printf("+------------+---------------------+------------+\n");
    printf("| Total records: %-30d |\n", count);
    printf("+------------+---------------------+------------+\n");
}

int main(int argc, char *argv[]) {
    const char *hdf5_file = NULL;
    uint32_t mesh_id = 0;
    char *single_time = NULL;
    char *start_time = NULL;
    char *end_time = NULL;
    int raw_output = 0;
    int opt;
    
    static struct option long_options[] = {
        {"file",  required_argument, 0, 'f'},
        {"mesh",  required_argument, 0, 'm'},
        {"time",  required_argument, 0, 't'},
        {"start", required_argument, 0, 's'},
        {"end",   required_argument, 0, 'e'},
        {"raw",   no_argument,       0, 'r'},
        {"help",  no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    while ((opt = getopt_long(argc, argv, "f:m:t:s:e:rh", long_options, NULL)) != -1) {
        switch (opt) {
            case 'f':
                hdf5_file = optarg;
                break;
            case 'm':
                mesh_id = (uint32_t)strtoul(optarg, NULL, 10);
                break;
            case 't':
                single_time = optarg;
                break;
            case 's':
                start_time = optarg;
                break;
            case 'e':
                end_time = optarg;
                break;
            case 'r':
                raw_output = 1;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }
    
    // Validate required arguments
    if (!hdf5_file || mesh_id == 0) {
        fprintf(stderr, "Error: Missing required arguments\n");
        print_usage(argv[0]);
        return 1;
    }
    
    // Check for conflicting options
    if (single_time && (start_time || end_time)) {
        fprintf(stderr, "Error: Cannot specify both single time and time range\n");
        print_usage(argv[0]);
        return 1;
    }
    
    if ((start_time && !end_time) || (!start_time && end_time)) {
        fprintf(stderr, "Error: Both start and end times must be specified for range query\n");
        print_usage(argv[0]);
        return 1;
    }
    
    if (!single_time && !start_time && !end_time) {
        fprintf(stderr, "Error: Must specify either single time (-t) or time range (-s and -e)\n");
        print_usage(argv[0]);
        return 1;
    }
    
    // Initialize h5mobaku
    struct h5mobaku *ctx = NULL;
    int ret = h5mobaku_open(hdf5_file, &ctx);
    if (ret != 0) {
        fprintf(stderr, "Error: Failed to open HDF5 file: %s\n", hdf5_file);
        return 1;
    }
    
    // Prepare mesh ID search
    cmph_t *hash = meshid_prepare_search();
    if (!hash) {
        fprintf(stderr, "Error: Failed to prepare mesh ID search\n");
        h5mobaku_close(ctx);
        return 1;
    }
    
    // Verify mesh ID exists
    uint32_t mesh_index = meshid_search_id(hash, mesh_id);
    if (mesh_index == MESHID_NOT_FOUND) {
        fprintf(stderr, "Error: Mesh ID %u not found\n", mesh_id);
        cmph_destroy(hash);
        h5mobaku_close(ctx);
        return 1;
    }
    
    // Perform query
    if (single_time) {
        // Single time query
        int32_t population = h5mobaku_read_population_single_at_time(ctx, hash, mesh_id, single_time);
        
        if (population < 0) {
            fprintf(stderr, "Error: Failed to read population data\n");
        } else {
            if (raw_output) {
                // Output raw uint32 value (Note: population is int32_t, but we cast to uint32_t for output)
                uint32_t raw_value = (uint32_t)population;
                fwrite(&raw_value, sizeof(uint32_t), 1, stdout);
            } else {
                print_table_header_single();
                print_table_row_single(mesh_id, single_time, population);
                print_table_footer_single();
            }
        }
    } else {
        // Time range query
        int32_t *time_series = h5mobaku_read_population_time_series_between(ctx, hash, mesh_id, start_time, end_time);
        
        if (!time_series) {
            fprintf(stderr, "Error: Failed to read time series data\n");
        } else {
            // Calculate time indices
            int start_idx = meshid_get_time_index_from_datetime(start_time);
            int end_idx = meshid_get_time_index_from_datetime(end_time);
            
            if (start_idx < 0 || end_idx < 0 || start_idx > end_idx) {
                fprintf(stderr, "Error: Invalid time range\n");
            } else {
                int count = end_idx - start_idx + 1;
                
                if (raw_output) {
                    // Output raw uint32 values as byte stream
                    for (int i = 0; i < count; i++) {
                        uint32_t raw_value = (uint32_t)time_series[i];
                        fwrite(&raw_value, sizeof(uint32_t), 1, stdout);
                    }
                } else {
                    print_table_header_range();
                    
                    // Print each time point
                    for (int i = 0; i < count; i++) {
                        char *datetime = meshid_get_datetime_from_time_index(start_idx + i);
                        if (datetime) {
                            print_table_row_range(mesh_id, datetime, time_series[i]);
                            free(datetime);
                        }
                    }
                    
                    print_table_footer_range(count);
                }
            }
            
            h5mobaku_free_data(time_series);
        }
    }
    
    // Cleanup
    cmph_destroy(hash);
    h5mobaku_close(ctx);
    
    return 0;
}