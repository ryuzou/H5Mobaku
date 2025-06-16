//
// H5M-Create: CLI tool for creating HDF5 files from CSV collections
// with optional VDS (Virtual Dataset) support for referencing existing data
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <hdf5.h>
#include "csv_to_h5_converter.h"
#include "csv_ops.h"
#include "meshid_ops.h"

typedef struct {
    char* output_file;
    char* csv_directory;
    char* csv_pattern;
    char* vds_source_file;
    int vds_cutoff_year;
    int batch_size;
    int verbose;
    int help;
    int use_bulk_write;
} h5m_create_config_t;

static void print_usage(const char* prog_name) {
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("\nCreate HDF5 files from CSV collections with optional VDS support\n");
    printf("\nOptions:\n");
    printf("  -o, --output <file>          Output HDF5 file path (required)\n");
    printf("  -d, --directory <path>       Directory containing CSV files (required)\n");
    printf("  -p, --pattern <pattern>      CSV file pattern (default: *.csv)\n");
    printf("  -v, --vds-source <file>      Reference dataset for VDS integration\n");
    printf("  -y, --vds-year <year>        Cutoff year for VDS reference (required with --vds-source)\n");
    printf("  -b, --batch-size <size>      Processing batch size (default: 10000)\n");
    printf("      --bulk-write             Enable year-wise bulk write mode (51 GiB memory)\n");
    printf("      --verbose                Enable verbose output\n");
    printf("  -h, --help                   Show this help message\n");
    
    printf("\nExamples:\n");
    printf("  Basic usage:\n");
    printf("    %s -o output.h5 -d /path/to/csv/files\n", prog_name);
    printf("  \n");
    printf("  With VDS for data before 2020:\n");
    printf("    %s -o output.h5 -d /path/to/csv/files -v existing.h5 -y 2020\n", prog_name);
    printf("  \n");
    printf("  With custom pattern and batch size:\n");
    printf("    %s -o output.h5 -d /path/to/csv -p \"data_*.csv\" -b 50000 --verbose\n", prog_name);
    printf("  \n");
    printf("  Year-wise bulk processing (requires 51 GiB RAM):\n");
    printf("    %s -o output.h5 -d /path/to/2024_csv --bulk-write --verbose\n", prog_name);
    
    printf("\nVirtual Dataset (VDS) Integration:\n");
    printf("  When --vds-source and --vds-year are specified, the output file will include\n");
    printf("  a virtual dataset that references data from the source file for all time\n");
    printf("  points before the specified year. New CSV data will be appended after the\n");
    printf("  VDS reference, creating a seamless time series that combines historical\n");
    printf("  and new data without duplicating the historical data.\n");
}

static int parse_arguments(int argc, char* argv[], h5m_create_config_t* config) {
    memset(config, 0, sizeof(h5m_create_config_t));
    
    // Set defaults
    config->csv_pattern = "*.csv";
    config->batch_size = 10000;
    config->vds_cutoff_year = -1;
    
    static struct option long_options[] = {
        {"output",      required_argument, 0, 'o'},
        {"directory",   required_argument, 0, 'd'},
        {"pattern",     required_argument, 0, 'p'},
        {"vds-source",  required_argument, 0, 'v'},
        {"vds-year",    required_argument, 0, 'y'},
        {"batch-size",  required_argument, 0, 'b'},
        {"bulk-write",  no_argument,       0, 1002},
        {"verbose",     no_argument,       0, 1001},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "o:d:p:v:y:b:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'o':
                config->output_file = strdup(optarg);
                break;
            case 'd':
                config->csv_directory = strdup(optarg);
                break;
            case 'p':
                config->csv_pattern = strdup(optarg);
                break;
            case 'v':
                config->vds_source_file = strdup(optarg);
                break;
            case 'y':
                config->vds_cutoff_year = atoi(optarg);
                break;
            case 'b':
                config->batch_size = atoi(optarg);
                if (config->batch_size <= 0) {
                    fprintf(stderr, "Error: Batch size must be positive\n");
                    return -1;
                }
                break;
            case 1001:
                config->verbose = 1;
                break;
            case 1002:
                config->use_bulk_write = 1;
                break;
            case 'h':
                config->help = 1;
                return 0;
            default:
                return -1;
        }
    }
    
    // Validate required arguments
    if (!config->output_file) {
        fprintf(stderr, "Error: Output file (-o) is required\n");
        return -1;
    }
    
    if (!config->csv_directory) {
        fprintf(stderr, "Error: CSV directory (-d) is required\n");
        return -1;
    }
    
    // Validate VDS options
    if (config->vds_source_file && config->vds_cutoff_year == -1) {
        fprintf(stderr, "Error: VDS year (-y) is required when VDS source (-v) is specified\n");
        return -1;
    }
    
    if (!config->vds_source_file && config->vds_cutoff_year != -1) {
        fprintf(stderr, "Error: VDS source (-v) is required when VDS year (-y) is specified\n");
        return -1;
    }
    
    // Validate VDS source file exists
    if (config->vds_source_file) {
        struct stat st;
        if (stat(config->vds_source_file, &st) != 0) {
            fprintf(stderr, "Error: VDS source file does not exist: %s\n", config->vds_source_file);
            return -1;
        }
    }
    
    // Validate CSV directory exists
    struct stat st;
    if (stat(config->csv_directory, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: CSV directory does not exist or is not a directory: %s\n", config->csv_directory);
        return -1;
    }
    
    return 0;
}

static void free_config(h5m_create_config_t* config) {
    if (config->output_file) free(config->output_file);
    if (config->csv_directory) free(config->csv_directory);
    if (config->csv_pattern && strcmp(config->csv_pattern, "*.csv") != 0) free(config->csv_pattern);
    if (config->vds_source_file) free(config->vds_source_file);
}

static int get_vds_time_dimensions(const char* vds_file, hsize_t* time_dim, hsize_t* mesh_dim) {
    hid_t file_id = H5Fopen(vds_file, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (file_id < 0) {
        fprintf(stderr, "Error: Cannot open VDS source file: %s\n", vds_file);
        return -1;
    }
    
    hid_t dataset_id = H5Dopen2(file_id, "/population_data", H5P_DEFAULT);
    if (dataset_id < 0) {
        fprintf(stderr, "Error: Cannot open population_data dataset in VDS source\n");
        H5Fclose(file_id);
        return -1;
    }
    
    hid_t space_id = H5Dget_space(dataset_id);
    if (space_id < 0) {
        H5Dclose(dataset_id);
        H5Fclose(file_id);
        return -1;
    }
    
    hsize_t dims[2];
    int ndims = H5Sget_simple_extent_dims(space_id, dims, NULL);
    if (ndims != 2) {
        fprintf(stderr, "Error: VDS source dataset must be 2D\n");
        H5Sclose(space_id);
        H5Dclose(dataset_id);
        H5Fclose(file_id);
        return -1;
    }
    
    *time_dim = dims[0];
    *mesh_dim = dims[1];
    
    H5Sclose(space_id);
    H5Dclose(dataset_id);
    H5Fclose(file_id);
    
    return 0;
}

static int filter_csv_files_by_year(char** all_files, size_t total_count, 
                                   char*** filtered_files, size_t* filtered_count,
                                   int cutoff_year) {
    *filtered_files = malloc(total_count * sizeof(char*));
    if (!*filtered_files) return -1;
    
    *filtered_count = 0;
    
    // For each CSV file, check if it contains data from cutoff_year or later
    for (size_t i = 0; i < total_count; i++) {
        csv_reader_t* reader = csv_open(all_files[i]);
        if (!reader) continue;
        
        csv_row_t row;
        int has_recent_data = 0;
        
        // Check first few rows to determine if file has data >= cutoff_year
        for (int j = 0; j < 10 && csv_read_row(reader, &row) == 0; j++) {
            int year = row.date / 10000;
            if (year >= cutoff_year) {
                has_recent_data = 1;
                break;
            }
        }
        
        csv_close(reader);
        
        if (has_recent_data) {
            (*filtered_files)[*filtered_count] = strdup(all_files[i]);
            (*filtered_count)++;
        }
    }
    
    return 0;
}

static int create_vds_integrated_file(const h5m_create_config_t* config, 
                                     char** csv_files, size_t csv_count,
                                     csv_to_h5_stats_t* stats) {
    if (config->verbose) {
        printf("Creating HDF5 file with VDS integration...\n");
        printf("VDS source: %s\n", config->vds_source_file);
        printf("VDS cutoff year: %d\n", config->vds_cutoff_year);
    }
    
    // Get dimensions from VDS source
    hsize_t vds_time_dim, vds_mesh_dim;
    if (get_vds_time_dimensions(config->vds_source_file, &vds_time_dim, &vds_mesh_dim) < 0) {
        return -1;
    }
    
    if (config->verbose) {
        printf("VDS source dimensions: %llu time points, %llu mesh IDs\n", 
               (unsigned long long)vds_time_dim, (unsigned long long)vds_mesh_dim);
    }
    
    // Convert CSV files directly to the output file using create mode
    // Create the new data with the desired dataset name directly
    csv_to_h5_config_t csv_config = CSV_TO_H5_DEFAULT_CONFIG;
    csv_config.output_h5_file = config->output_file;
    csv_config.dataset_name = "/population_new"; // Create directly as /population_new
    csv_config.batch_size = config->batch_size;
    csv_config.verbose = config->verbose;
    csv_config.create_new = 1; // Create new file
    csv_config.use_bulk_write = config->use_bulk_write;
    
    if (config->verbose) {
        printf("Converting %zu CSV files directly to output file...\n", csv_count);
    }
    
    int result = csv_to_h5_convert_files((const char**)csv_files, csv_count, &csv_config, stats);
    if (result != 0) {
        fprintf(stderr, "Error: Failed to convert CSV files\n");
        return -1;
    }
    
    // Reopen the file to get new data dimensions
    hid_t output_file = H5Fopen(config->output_file, H5F_ACC_RDWR, H5P_DEFAULT);
    if (output_file < 0) {
        fprintf(stderr, "Error: Cannot reopen output file: %s\n", config->output_file);
        return -1;
    }
    
    // Get new data dimensions from the created /population_new dataset
    hid_t new_dataset = H5Dopen2(output_file, "/population_new", H5P_DEFAULT);
    if (new_dataset < 0) {
        fprintf(stderr, "Error: Cannot open /population_new dataset\n");
        H5Fclose(output_file);
        return -1;
    }
    
    hid_t new_dataspace = H5Dget_space(new_dataset);
    hsize_t actual_dims[2];
    H5Sget_simple_extent_dims(new_dataspace, actual_dims, NULL);
    
    hsize_t new_time_dim = actual_dims[0];
    hsize_t new_mesh_dim = actual_dims[1];
    
    H5Sclose(new_dataspace);
    H5Dclose(new_dataset);
    
    if (config->verbose) {
        printf("New data dimensions: %llu time points, %llu mesh IDs\n",
               (unsigned long long)new_time_dim, (unsigned long long)new_mesh_dim);
        printf("Dataset created as /population_new\n");
    }
    
    // Calculate combined dataset dimensions
    hsize_t total_time_dim = vds_time_dim + new_time_dim;
    hsize_t total_mesh_dim = (new_mesh_dim > vds_mesh_dim) ? new_mesh_dim : vds_mesh_dim;
    
    hsize_t dims[2] = {total_time_dim, total_mesh_dim};
    hsize_t maxdims[2] = {H5S_UNLIMITED, total_mesh_dim};
    
    // Create dataspace for the combined VDS dataset
    hid_t space_id = H5Screate_simple(2, dims, maxdims);
    
    // Create dataset creation property list with correct chunking
    hid_t dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    
    // Use existing chunking configuration (from h5mr.h)
    // Use larger chunk size to accommodate leap years (366 * 24 = 8784 hours)
    hsize_t chunk_dims[2] = {8784, 16}; // chunk_time_size=8784 (leap year), chunk_mesh_size=16
    H5Pset_chunk(dcpl_id, 2, chunk_dims);
    
    // Set fill value
    int32_t vds_fill_value = 0;
    H5Pset_fill_value(dcpl_id, H5T_NATIVE_INT32, &vds_fill_value);
    
    // *** CRITICAL: Set up VDS mapping BEFORE creating the dataset ***
    
    // Create virtual mapping for historical data (first part of the dataset)
    hsize_t vds_start[2] = {0, 0};
    hsize_t vds_count[2] = {vds_time_dim, vds_mesh_dim};
    
    // Create dataspace for the virtual mapping in the target dataset
    hid_t vds_target_space = H5Screate_simple(2, dims, maxdims);
    H5Sselect_hyperslab(vds_target_space, H5S_SELECT_SET, vds_start, NULL, vds_count, NULL);
    
    // Create dataspace for the source dataset 
    hsize_t src_dims[2] = {vds_time_dim, vds_mesh_dim};
    hid_t vds_source_space = H5Screate_simple(2, src_dims, NULL);
    
    // Set up the virtual mapping for historical data
    if (H5Pset_virtual(dcpl_id, vds_target_space, config->vds_source_file, "/population_data", vds_source_space) < 0) {
        fprintf(stderr, "Error: Failed to set VDS mapping for historical data\n");
        H5Sclose(vds_source_space);
        H5Sclose(vds_target_space);
        H5Pclose(dcpl_id);
        H5Sclose(space_id);
        H5Fclose(output_file);
        return -1;
    }
    
    H5Sclose(vds_source_space);
    H5Sclose(vds_target_space);
    
    // Set up virtual mapping for new data (second part of the dataset) - SELF-REFERENCE
    hsize_t new_start[2] = {vds_time_dim, 0};
    hsize_t new_count[2] = {new_time_dim, new_mesh_dim};
    
    // Create dataspace for the new data mapping in the target dataset
    hid_t new_target_space = H5Screate_simple(2, dims, maxdims);
    H5Sselect_hyperslab(new_target_space, H5S_SELECT_SET, new_start, NULL, new_count, NULL);
    
    // Create dataspace for the new data source
    hsize_t new_src_dims[2] = {new_time_dim, new_mesh_dim};
    hid_t new_source_space = H5Screate_simple(2, new_src_dims, NULL);
    
    // Set up the virtual mapping for new data (self-reference to /population_new)
    if (H5Pset_virtual(dcpl_id, new_target_space, config->output_file, "/population_new", new_source_space) < 0) {
        fprintf(stderr, "Error: Failed to set VDS mapping for new data\n");
        H5Sclose(new_source_space);
        H5Sclose(new_target_space);
        H5Pclose(dcpl_id);
        H5Sclose(space_id);
        H5Fclose(output_file);
        return -1;
    }
    
    H5Sclose(new_source_space);
    H5Sclose(new_target_space);
    
    // Now create the virtual dataset with the configured property list
    hid_t dataset_id = H5Dcreate2(output_file, "/population_data", H5T_NATIVE_INT32, 
                                 space_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    if (dataset_id < 0) {
        fprintf(stderr, "Error: Failed to create VDS population_data dataset\n");
        H5Pclose(dcpl_id);
        H5Sclose(space_id);
        H5Fclose(output_file);
        return -1;
    }
    
    // Copy attributes from the VDS source file
    hid_t src_file = H5Fopen(config->vds_source_file, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (src_file >= 0) {
        hid_t src_dataset = H5Dopen2(src_file, "/population_data", H5P_DEFAULT);
        if (src_dataset >= 0) {
            // Copy start_datetime attribute if it exists
            if (H5Aexists(src_dataset, "start_datetime") > 0) {
                hid_t src_attr = H5Aopen(src_dataset, "start_datetime", H5P_DEFAULT);
                if (src_attr >= 0) {
                    hid_t attr_type = H5Aget_type(src_attr);
                    hid_t attr_space = H5Aget_space(src_attr);
                    
                    hid_t new_attr = H5Acreate2(dataset_id, "start_datetime", attr_type, attr_space,
                                               H5P_DEFAULT, H5P_DEFAULT);
                    if (new_attr >= 0) {
                        // Read and copy attribute data
                        if (H5Tget_class(attr_type) == H5T_STRING) {
                            size_t attr_size = H5Tget_size(attr_type);
                            char* attr_data = malloc(attr_size + 1);
                            if (attr_data) {
                                if (H5Aread(src_attr, attr_type, attr_data) >= 0) {
                                    H5Awrite(new_attr, attr_type, attr_data);
                                }
                                free(attr_data);
                            }
                        }
                        H5Aclose(new_attr);
                    }
                    
                    H5Sclose(attr_space);
                    H5Tclose(attr_type);
                    H5Aclose(src_attr);
                }
            }
            H5Dclose(src_dataset);
        }
        H5Fclose(src_file);
    }
    
    // Cleanup
    H5Dclose(dataset_id);
    H5Pclose(dcpl_id);
    H5Sclose(space_id);
    H5Fclose(output_file);
    
    if (config->verbose) {
        printf("VDS integration completed successfully\n");
        printf("Total time points: %llu (VDS: %llu, New: %llu)\n",
               (unsigned long long)total_time_dim, 
               (unsigned long long)vds_time_dim,
               (unsigned long long)new_time_dim);
        printf("Structure: /population_data (VDS) -> /population_new (self) + %s (external)\n", 
               config->vds_source_file);
    }
    
    return 0;
}

int main(int argc, char* argv[]) {
    h5m_create_config_t config;
    
    // Parse command line arguments
    int result = parse_arguments(argc, argv, &config);
    if (result < 0) {
        print_usage(argv[0]);
        return 1;
    }
    
    if (config.help) {
        print_usage(argv[0]);
        free_config(&config);
        return 0;
    }
    
    if (config.verbose) {
        printf("H5M-Create: Creating HDF5 from CSV collection\n");
        printf("Output file: %s\n", config.output_file);
        printf("CSV directory: %s\n", config.csv_directory);
        printf("CSV pattern: %s\n", config.csv_pattern);
        printf("Batch size: %d\n", config.batch_size);
        if (config.use_bulk_write) {
            printf("Bulk write mode: ENABLED (requires 51 GiB RAM)\n");
        }
        if (config.vds_source_file) {
            printf("VDS source: %s (cutoff year: %d)\n", config.vds_source_file, config.vds_cutoff_year);
        }
    }
    
    // Find all CSV files
    char** all_csv_files = malloc(10000 * sizeof(char*));
    size_t total_file_count = 0;
    size_t file_capacity = 10000;
    
    find_csv_files(config.csv_directory, &all_csv_files, &total_file_count, &file_capacity);
    
    if (total_file_count == 0) {
        fprintf(stderr, "Error: No CSV files found in directory: %s\n", config.csv_directory);
        free(all_csv_files);
        free_config(&config);
        return 1;
    }
    
    if (config.verbose) {
        printf("Found %zu CSV files\n", total_file_count);
    }
    
    csv_to_h5_stats_t stats;
    
    if (config.vds_source_file) {
        // Filter CSV files to only include data from cutoff year or later
        char** filtered_files;
        size_t filtered_count;
        
        if (filter_csv_files_by_year(all_csv_files, total_file_count, 
                                   &filtered_files, &filtered_count, 
                                   config.vds_cutoff_year) < 0) {
            fprintf(stderr, "Error: Failed to filter CSV files by year\n");
            for (size_t i = 0; i < total_file_count; i++) {
                free(all_csv_files[i]);
            }
            free(all_csv_files);
            free_config(&config);
            return 1;
        }
        
        if (config.verbose) {
            printf("Filtered to %zu CSV files with data >= %d\n", filtered_count, config.vds_cutoff_year);
        }
        
        result = create_vds_integrated_file(&config, filtered_files, filtered_count, &stats);
        
        // Cleanup filtered files
        for (size_t i = 0; i < filtered_count; i++) {
            free(filtered_files[i]);
        }
        free(filtered_files);
    } else {
        // Standard conversion without VDS
        csv_to_h5_config_t csv_config = CSV_TO_H5_DEFAULT_CONFIG;
        csv_config.output_h5_file = config.output_file;
        csv_config.batch_size = config.batch_size;
        csv_config.verbose = config.verbose;
        csv_config.create_new = 1;
        csv_config.use_bulk_write = config.use_bulk_write;
        
        result = csv_to_h5_convert_files((const char**)all_csv_files, total_file_count, &csv_config, &stats);
    }
    
    // Cleanup
    for (size_t i = 0; i < total_file_count; i++) {
        free(all_csv_files[i]);
    }
    free(all_csv_files);
    
    if (result == 0) {
        printf("\nConversion completed successfully!\n");
        printf("Output file: %s\n", config.output_file);
        printf("Total rows processed: %zu\n", stats.total_rows_processed);
        printf("Unique timestamps: %zu\n", stats.unique_timestamps);
        printf("Errors: %zu\n", stats.errors);
    } else {
        fprintf(stderr, "Error: Conversion failed\n");
        free_config(&config);
        return 1;
    }
    
    free_config(&config);
    return 0;
}