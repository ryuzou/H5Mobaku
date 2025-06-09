//
// CSV operations for reading population data CSV files
//

#ifndef CSV_OPS_H
#define CSV_OPS_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <time.h>
#include "fifioq.h"

#ifdef __cplusplus
extern "C" {
#endif

// CSV row structure
typedef struct {
    uint32_t date;      // YYYYMMDD
    uint16_t time;      // HHMM
    uint64_t area;      // mesh ID
    int32_t residence;  // -1 or actual value
    int32_t age;        // -1 or actual value
    int32_t gender;     // -1 or actual value
    int32_t population; // population count
} csv_row_t;

// CSV reader context
typedef struct csv_reader csv_reader_t;

// Open CSV file for reading
csv_reader_t* csv_open(const char* filename);

// Read next row from CSV
int csv_read_row(csv_reader_t* reader, csv_row_t* row);

// Get current line number (for error reporting)
size_t csv_get_line_number(const csv_reader_t* reader);

// Close CSV reader
void csv_close(csv_reader_t* reader);

// Parse CSV header and validate format
int csv_validate_header(csv_reader_t* reader);

// Population data structure for queue
typedef struct {
    uint64_t meshid;        // mesh ID (area field)
    time_t datetime;        // combined date and time
    int32_t population;     // population count
    char* source_file;      // source CSV file (for debugging)
} population_data_t;

// CSV reader thread data structure
typedef struct {
    int thread_id;
    char** filepaths;
    size_t num_files;
    FIFOQueue* queue;
    size_t* rows_processed;
    pthread_mutex_t* stats_mutex;
} csv_reader_thread_data_t;

// CSV reader thread function
void* csv_reader_thread_func(void* arg);

// Convert date (YYYYMMDD) and time (HHMM) to time_t
time_t csv_datetime_to_time_t(uint32_t date, uint16_t time);

// Free population data
void free_population_data(population_data_t* data);

#ifdef __cplusplus
}
#endif

#endif // CSV_OPS_H