//
// CSV operations implementation
//

#include "csv_ops.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#define CSV_BUFFER_SIZE 1024

struct csv_reader {
    FILE* file;
    char buffer[CSV_BUFFER_SIZE];
    size_t line_number;
    int header_validated;
};

csv_reader_t* csv_open(const char* filename) {
    if (!filename) return NULL;
    
    csv_reader_t* reader = calloc(1, sizeof(csv_reader_t));
    if (!reader) return NULL;
    
    reader->file = fopen(filename, "r");
    if (!reader->file) {
        free(reader);
        return NULL;
    }
    
    reader->line_number = 0;
    reader->header_validated = 0;
    return reader;
}

void csv_close(csv_reader_t* reader) {
    if (!reader) return;
    if (reader->file) fclose(reader->file);
    free(reader);
}

size_t csv_get_line_number(const csv_reader_t* reader) {
    return reader ? reader->line_number : 0;
}

int csv_validate_header(csv_reader_t* reader) {
    if (!reader || !reader->file) return -1;
    if (reader->header_validated) return 0;
    
    // Read header line
    if (!fgets(reader->buffer, CSV_BUFFER_SIZE, reader->file)) {
        return -1;
    }
    reader->line_number++;
    
    // Remove newline
    size_t len = strlen(reader->buffer);
    if (len > 0 && reader->buffer[len-1] == '\n') {
        reader->buffer[len-1] = '\0';
    }
    
    // Expected header
    const char* expected = "date,time,area,residence,age,gender,population";
    
    if (strcmp(reader->buffer, expected) != 0) {
        return -1;
    }
    
    reader->header_validated = 1;
    return 0;
}

static int parse_int32(const char* str, int32_t* value) {
    if (!str || !value) return -1;
    
    char* endptr;
    errno = 0;
    long val = strtol(str, &endptr, 10);
    
    if (errno != 0 || endptr == str || *endptr != '\0') {
        return -1;
    }
    
    if (val < INT32_MIN || val > INT32_MAX) {
        return -1;
    }
    
    *value = (int32_t)val;
    return 0;
}

static int parse_uint32(const char* str, uint32_t* value) {
    if (!str || !value) return -1;
    
    char* endptr;
    errno = 0;
    unsigned long val = strtoul(str, &endptr, 10);
    
    if (errno != 0 || endptr == str || *endptr != '\0') {
        return -1;
    }
    
    if (val > UINT32_MAX) {
        return -1;
    }
    
    *value = (uint32_t)val;
    return 0;
}

static int parse_uint16(const char* str, uint16_t* value) {
    if (!str || !value) return -1;
    
    char* endptr;
    errno = 0;
    unsigned long val = strtoul(str, &endptr, 10);
    
    if (errno != 0 || endptr == str || *endptr != '\0') {
        return -1;
    }
    
    if (val > UINT16_MAX) {
        return -1;
    }
    
    *value = (uint16_t)val;
    return 0;
}

static int parse_uint64(const char* str, uint64_t* value) {
    if (!str || !value) return -1;
    
    char* endptr;
    errno = 0;
    unsigned long long val = strtoull(str, &endptr, 10);
    
    if (errno != 0 || endptr == str || *endptr != '\0') {
        return -1;
    }
    
    *value = (uint64_t)val;
    return 0;
}

int csv_read_row(csv_reader_t* reader, csv_row_t* row) {
    if (!reader || !reader->file || !row) return -1;
    
    // Ensure header is validated
    if (!reader->header_validated) {
        if (csv_validate_header(reader) < 0) {
            return -1;
        }
    }
    
    // Read next line
    if (!fgets(reader->buffer, CSV_BUFFER_SIZE, reader->file)) {
        return feof(reader->file) ? 1 : -1;  // 1 for EOF, -1 for error
    }
    reader->line_number++;
    
    // Remove newline
    size_t len = strlen(reader->buffer);
    if (len > 0 && reader->buffer[len-1] == '\n') {
        reader->buffer[len-1] = '\0';
    }
    
    // Parse CSV fields
    char* saveptr;
    char* token;
    int field_count = 0;
    
    // date
    token = strtok_r(reader->buffer, ",", &saveptr);
    if (!token || parse_uint32(token, &row->date) < 0) return -1;
    field_count++;
    
    // time
    token = strtok_r(NULL, ",", &saveptr);
    if (!token || parse_uint16(token, &row->time) < 0) return -1;
    field_count++;
    
    // area (mesh ID)
    token = strtok_r(NULL, ",", &saveptr);
    if (!token || parse_uint64(token, &row->area) < 0) return -1;
    field_count++;
    
    // residence
    token = strtok_r(NULL, ",", &saveptr);
    if (!token || parse_int32(token, &row->residence) < 0) return -1;
    field_count++;
    
    // age
    token = strtok_r(NULL, ",", &saveptr);
    if (!token || parse_int32(token, &row->age) < 0) return -1;
    field_count++;
    
    // gender
    token = strtok_r(NULL, ",", &saveptr);
    if (!token || parse_int32(token, &row->gender) < 0) return -1;
    field_count++;
    
    // population
    token = strtok_r(NULL, ",", &saveptr);
    if (!token || parse_int32(token, &row->population) < 0) return -1;
    field_count++;
    
    // Ensure no extra fields
    token = strtok_r(NULL, ",", &saveptr);
    if (token) return -1;
    
    return 0;  // Success
}

time_t csv_datetime_to_time_t(uint32_t date, uint16_t time) {
    struct tm tm_info = {0};
    
    // Extract date components
    tm_info.tm_year = (date / 10000) - 1900;  // Year since 1900
    tm_info.tm_mon = ((date / 100) % 100) - 1; // Month 0-11
    tm_info.tm_mday = date % 100;              // Day 1-31
    
    // Extract time components
    tm_info.tm_hour = time / 100;              // Hour 0-23
    tm_info.tm_min = time % 100;               // Minute 0-59
    tm_info.tm_sec = 0;                        // Second 0
    
    return mktime(&tm_info);
}

void free_population_data(population_data_t* data) {
    if (data) {
        if (data->source_file) {
            free(data->source_file);
        }
        free(data);
    }
}

void* csv_reader_thread_func(void* arg) {
    csv_reader_thread_data_t* data = (csv_reader_thread_data_t*)arg;
    
    printf("CSV reader thread %d started, processing %zu files\n", 
           data->thread_id, data->num_files);
    
    for (size_t i = 0; i < data->num_files; i++) {
        const char* filepath = data->filepaths[i];
        csv_reader_t* reader = csv_open(filepath);
        
        if (!reader) {
            fprintf(stderr, "Thread %d: Failed to open %s\n", 
                    data->thread_id, filepath);
            continue;
        }
        
        csv_row_t row;
        size_t row_count = 0;
        
        while (csv_read_row(reader, &row) == 0) {
            // Create population data entry
            population_data_t* pop_data = malloc(sizeof(population_data_t));
            if (!pop_data) {
                fprintf(stderr, "Thread %d: Failed to allocate memory\n", 
                        data->thread_id);
                break;
            }
            
            pop_data->meshid = row.area;
            pop_data->datetime = csv_datetime_to_time_t(row.date, row.time);
            pop_data->population = row.population;
            pop_data->source_file = strdup(filepath);
            
            // Enqueue the data
            enqueue(data->queue, pop_data);
            row_count++;
        }
        
        csv_close(reader);
        
        // Update statistics
        pthread_mutex_lock(data->stats_mutex);
        (*data->rows_processed) += row_count;
        pthread_mutex_unlock(data->stats_mutex);
        
        printf("Thread %d: Read %zu rows from %s\n", 
               data->thread_id, row_count, filepath);

    }
    
    printf("CSV reader thread %d finished\n", data->thread_id);
    return NULL;
}

void find_csv_files(const char* dir_path, char*** files, size_t* count, size_t* capacity) {
    DIR* dir = opendir(dir_path);
    if (!dir) return;
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        
        char full_path[1024];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
        
        struct stat st;
        if (stat(full_path, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                find_csv_files(full_path, files, count, capacity);
            } else if (S_ISREG(st.st_mode)) {
                size_t len = strlen(entry->d_name);
                if (len > 4 && strcmp(entry->d_name + len - 4, ".csv") == 0) {
                    if (*count >= *capacity) {
                        *capacity *= 2;
                        *files = realloc(*files, *capacity * sizeof(char*));
                    }
                    (*files)[*count] = strdup(full_path);
                    (*count)++;
                }
            }
        }
    }
    
    closedir(dir);
}