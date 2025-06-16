//
// CSV to HDF5 converter for population data
//

#ifndef CSV_TO_H5_CONVERTER_H
#define CSV_TO_H5_CONVERTER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Converter configuration
typedef struct {
    const char* output_h5_file;     // Output HDF5 filename
    const char* dataset_name;       // Custom dataset name (default: "/population_data")
    size_t batch_size;              // Number of rows to process in batch
    int verbose;                    // Enable verbose output
    int create_new;                 // Create new file (1) or append (0)
    int use_bulk_write;             // Use bulk write mode for year-wise processing
} csv_to_h5_config_t;

// Default configuration
#define CSV_TO_H5_DEFAULT_CONFIG { \
    .output_h5_file = "population_debug.h5", \
    .dataset_name = "/population_data", \
    .batch_size = 10000, \
    .verbose = 0, \
    .create_new = 1, \
    .use_bulk_write = 0 \
}

// Converter statistics
typedef struct {
    size_t total_rows_processed;
    size_t unique_timestamps;
    size_t unique_meshes;
    size_t errors;
} csv_to_h5_stats_t;

// Convert single CSV file to HDF5
int csv_to_h5_convert_file(const char* csv_filename, const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats);

// Convert multiple CSV files to HDF5
int csv_to_h5_convert_files(const char** csv_filenames, size_t num_files, 
                           const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats);

// Convert all CSV files matching pattern in directory
int csv_to_h5_convert_directory(const char* directory, const char* pattern,
                               const csv_to_h5_config_t* config, csv_to_h5_stats_t* stats);

#ifdef __cplusplus
}
#endif

#endif // CSV_TO_H5_CONVERTER_H