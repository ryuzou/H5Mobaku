//
// CSV to HDF5 CLI application
//

#include "csv_to_h5_converter.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>

static void print_usage(const char* prog_name) {
    printf("Usage: %s [OPTIONS] <csv_file1> [csv_file2 ...]\n", prog_name);
    printf("       %s [OPTIONS] -d <directory> -p <pattern>\n", prog_name);
    printf("\n");
    printf("Convert CSV population data files to HDF5 format.\n");
    printf("\n");
    printf("Options:\n");
    printf("  -o, --output <file>      Output HDF5 file (default: population_debug.h5)\n");
    printf("  -b, --batch-size <size>  Batch size for processing (default: 10000)\n");
    printf("  -d, --directory <dir>    Process all CSV files in directory\n");
    printf("  -p, --pattern <pattern>  File pattern to match (default: *.csv)\n");
    printf("  -a, --append             Append to existing HDF5 file\n");
    printf("  -v, --verbose            Enable verbose output\n");
    printf("  -h, --help               Show this help message\n");
    printf("\n");
    printf("Examples:\n");
    printf("  %s data.csv\n", prog_name);
    printf("  %s -o output.h5 file1.csv file2.csv\n", prog_name);
    printf("  %s -d ./data -p \"*_mesh_pop_*.csv\"\n", prog_name);
    printf("  %s -a -o existing.h5 new_data.csv\n", prog_name);
}

static void print_stats(const csv_to_h5_stats_t* stats) {
    printf("\nConversion Statistics:\n");
    printf("  Total rows processed: %zu\n", stats->total_rows_processed);
    printf("  Unique timestamps:    %zu\n", stats->unique_timestamps);
    printf("  Unique meshes:        %zu\n", stats->unique_meshes);
    printf("  Errors:               %zu\n", stats->errors);
}

int main(int argc, char* argv[]) {
    csv_to_h5_config_t config = CSV_TO_H5_DEFAULT_CONFIG;
    char* directory = NULL;
    char* pattern = "*.csv";
    int append_mode = 0;
    
    // Long options
    static struct option long_options[] = {
        {"output",     required_argument, 0, 'o'},
        {"batch-size", required_argument, 0, 'b'},
        {"directory",  required_argument, 0, 'd'},
        {"pattern",    required_argument, 0, 'p'},
        {"append",     no_argument,       0, 'a'},
        {"verbose",    no_argument,       0, 'v'},
        {"help",       no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    // Parse options
    int opt;
    int option_index = 0;
    
    while ((opt = getopt_long(argc, argv, "o:b:d:p:avh", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'o':
                config.output_h5_file = optarg;
                break;
                
            case 'b':
                config.batch_size = strtoul(optarg, NULL, 10);
                if (config.batch_size == 0) {
                    fprintf(stderr, "Error: Invalid batch size\n");
                    return 1;
                }
                break;
                
            case 'd':
                directory = optarg;
                break;
                
            case 'p':
                pattern = optarg;
                break;
                
            case 'a':
                append_mode = 1;
                break;
                
            case 'v':
                config.verbose = 1;
                break;
                
            case 'h':
                print_usage(argv[0]);
                return 0;
                
            default:
                print_usage(argv[0]);
                return 1;
        }
    }
    
    config.create_new = !append_mode;
    
    // Check if we have input files
    if (directory) {
        // Directory mode
        if (optind < argc) {
            fprintf(stderr, "Error: Cannot specify both directory and individual files\n");
            return 1;
        }
        
        if (!access(directory, R_OK)) {
            if (config.verbose) {
                printf("Processing directory: %s\n", directory);
                printf("Pattern: %s\n", pattern);
                printf("Output file: %s\n", config.output_h5_file);
            }
            
            csv_to_h5_stats_t stats = {0};
            int result = csv_to_h5_convert_directory(directory, pattern, &config, &stats);
            
            if (result < 0) {
                fprintf(stderr, "Error: Failed to process directory\n");
                return 1;
            }
            
            print_stats(&stats);
            return 0;
        } else {
            fprintf(stderr, "Error: Cannot access directory: %s\n", directory);
            return 1;
        }
    } else {
        // File mode
        if (optind >= argc) {
            fprintf(stderr, "Error: No input files specified\n");
            print_usage(argv[0]);
            return 1;
        }
        
        int num_files = argc - optind;
        char** filenames = &argv[optind];
        
        // Verify all files exist
        for (int i = 0; i < num_files; i++) {
            if (access(filenames[i], R_OK) != 0) {
                fprintf(stderr, "Error: Cannot access file: %s\n", filenames[i]);
                return 1;
            }
        }
        
        if (config.verbose) {
            printf("Processing %d file(s)\n", num_files);
            printf("Output file: %s\n", config.output_h5_file);
            printf("Mode: %s\n", append_mode ? "append" : "create new");
        }
        
        csv_to_h5_stats_t stats = {0};
        int result = csv_to_h5_convert_files((const char**)filenames, num_files, &config, &stats);
        
        if (result < 0) {
            fprintf(stderr, "Error: Conversion failed\n");
            return 1;
        }
        
        print_stats(&stats);
        return 0;
    }
}