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

// SIMD optimization includes
#ifdef __AVX512F__
#include <immintrin.h>
#define SIMD_ENABLED 1
#define SIMD_WIDTH 64
#define AVX512_ENABLED 1
#elif defined(__AVX2__)
#include <immintrin.h>
#define SIMD_ENABLED 1
#define SIMD_WIDTH 32
#define AVX512_ENABLED 0
#elif defined(__SSE2__)
#include <emmintrin.h>
#define SIMD_ENABLED 1
#define SIMD_WIDTH 16
#define AVX512_ENABLED 0
#else
#define SIMD_ENABLED 0
#define SIMD_WIDTH 1
#define AVX512_ENABLED 0
#endif

#define CSV_BUFFER_SIZE 1024

struct csv_reader {
    FILE* file;
    char buffer[CSV_BUFFER_SIZE];
    size_t line_number;
    int header_validated;
};

// Forward declarations for parse functions
static int parse_int32(const char* str, int32_t* value);
static int parse_uint32(const char* str, uint32_t* value);
static int parse_uint16(const char* str, uint16_t* value);
static int parse_uint64(const char* str, uint64_t* value);

#if SIMD_ENABLED
// SIMD-optimized comma detection for faster CSV parsing
static inline int find_commas_simd(const char* line, size_t len, int* comma_positions, int max_commas) {
    int comma_count = 0;
    size_t pos = 0;
    
#ifdef __AVX512F__
    // AVX-512 version: process 64 bytes at a time
    const __m512i comma_pattern = _mm512_set1_epi8(',');
    
    while (pos + 63 < len && comma_count < max_commas) {
        __m512i chunk = _mm512_loadu_si512((const __m512i*)(line + pos));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(chunk, comma_pattern);
        
        // Extract comma positions from mask
        while (mask && comma_count < max_commas) {
            int bit_pos = __builtin_ctzll(mask);  // Count trailing zeros
            comma_positions[comma_count++] = pos + bit_pos;
            mask &= mask - 1;  // Clear lowest set bit
        }
        
        pos += 64;
    }
#elif defined(__AVX2__)
    // AVX2 version: process 32 bytes at a time
    const __m256i comma_pattern = _mm256_set1_epi8(',');
    
    while (pos + 31 < len && comma_count < max_commas) {
        __m256i chunk = _mm256_loadu_si256((const __m256i*)(line + pos));
        __m256i comparison = _mm256_cmpeq_epi8(chunk, comma_pattern);
        uint32_t mask = _mm256_movemask_epi8(comparison);
        
        // Extract comma positions from mask
        while (mask && comma_count < max_commas) {
            int bit_pos = __builtin_ctz(mask);  // Count trailing zeros
            comma_positions[comma_count++] = pos + bit_pos;
            mask &= mask - 1;  // Clear lowest set bit
        }
        
        pos += 32;
    }
#elif defined(__SSE2__)
    // SSE2 version: process 16 bytes at a time
    const __m128i comma_pattern = _mm_set1_epi8(',');
    
    while (pos + 15 < len && comma_count < max_commas) {
        __m128i chunk = _mm_loadu_si128((const __m128i*)(line + pos));
        __m128i comparison = _mm_cmpeq_epi8(chunk, comma_pattern);
        uint16_t mask = _mm_movemask_epi8(comparison);
        
        // Extract comma positions from mask
        while (mask && comma_count < max_commas) {
            int bit_pos = __builtin_ctz(mask);  // Count trailing zeros
            comma_positions[comma_count++] = pos + bit_pos;
            mask &= mask - 1;  // Clear lowest set bit
        }
        
        pos += 16;
    }
#endif
    
    // Handle remaining bytes with scalar processing
    while (pos < len && comma_count < max_commas) {
        if (line[pos] == ',') {
            comma_positions[comma_count++] = pos;
        }
        pos++;
    }
    
    return comma_count;
}

#if AVX512_ENABLED
// AVX-512 optimized memcpy for small strings
static inline void fast_memcpy_avx512(char* dst, const char* src, size_t len) {
    if (len <= 64) {
        // Use AVX-512 for up to 64 bytes
        __m512i data = _mm512_loadu_si512((const __m512i*)src);
        _mm512_storeu_si512((__m512i*)dst, data);
    } else {
        memcpy(dst, src, len);
    }
}

// AVX-512 optimized string to integer conversion for decimal numbers
static inline int parse_int32_avx512(const char* str, size_t len, int32_t* value) {
    if (len == 0 || len > 10) return -1;  // int32 max is 10 digits
    
    // Load up to 16 characters (more than needed for int32)
    __m128i chars = _mm_loadu_si128((const __m128i*)str);
    
    // Check for valid digit range (0x30-0x39)
    __m128i digit_min = _mm_set1_epi8('0');
    __m128i digit_max = _mm_set1_epi8('9');
    // Use equivalent operations: a >= b is !(a < b), a <= b is !(a > b)
    __m128i valid_min = _mm_xor_si128(_mm_cmplt_epi8(chars, digit_min), _mm_set1_epi8(-1));
    __m128i valid_max = _mm_xor_si128(_mm_cmpgt_epi8(chars, digit_max), _mm_set1_epi8(-1));
    __m128i valid = _mm_and_si128(valid_min, valid_max);
    
    // Create mask for valid length
    __m128i len_mask = _mm_set_epi8(
        len > 15 ? 0xFF : 0, len > 14 ? 0xFF : 0, len > 13 ? 0xFF : 0, len > 12 ? 0xFF : 0,
        len > 11 ? 0xFF : 0, len > 10 ? 0xFF : 0, len > 9 ? 0xFF : 0, len > 8 ? 0xFF : 0,
        len > 7 ? 0xFF : 0, len > 6 ? 0xFF : 0, len > 5 ? 0xFF : 0, len > 4 ? 0xFF : 0,
        len > 3 ? 0xFF : 0, len > 2 ? 0xFF : 0, len > 1 ? 0xFF : 0, len > 0 ? 0xFF : 0
    );
    
    valid = _mm_and_si128(valid, len_mask);
    
    // Check if all required characters are valid digits
    uint16_t mask = _mm_movemask_epi8(valid);
    if ((mask & ((1 << len) - 1)) != ((1 << len) - 1)) {
        return -1;  // Invalid characters found
    }
    
    // Convert to integer using traditional method (SIMD conversion is complex)
    int32_t result = 0;
    for (size_t i = 0; i < len; i++) {
        result = result * 10 + (str[i] - '0');
    }
    
    *value = result;
    return 0;
}

// AVX-512 optimized string to uint64 conversion
static inline int parse_uint64_avx512(const char* str, size_t len, uint64_t* value) {
    if (len == 0 || len > 20) return -1;  // uint64 max is 20 digits
    
    // Use similar validation as int32 but for longer strings
    if (len <= 16) {
        __m128i chars = _mm_loadu_si128((const __m128i*)str);
        __m128i digit_min = _mm_set1_epi8('0');
        __m128i digit_max = _mm_set1_epi8('9');
        // Use equivalent operations: a >= b is !(a < b), a <= b is !(a > b)
        __m128i valid_min = _mm_xor_si128(_mm_cmplt_epi8(chars, digit_min), _mm_set1_epi8(-1));
        __m128i valid_max = _mm_xor_si128(_mm_cmpgt_epi8(chars, digit_max), _mm_set1_epi8(-1));
        __m128i valid = _mm_and_si128(valid_min, valid_max);
        
        uint16_t mask = _mm_movemask_epi8(valid);
        if ((mask & ((1 << len) - 1)) != ((1 << len) - 1)) {
            return -1;
        }
    } else {
        // Fallback for longer strings
        for (size_t i = 0; i < len; i++) {
            if (str[i] < '0' || str[i] > '9') return -1;
        }
    }
    
    // Convert to integer
    uint64_t result = 0;
    for (size_t i = 0; i < len; i++) {
        result = result * 10 + (str[i] - '0');
    }
    
    *value = result;
    return 0;
}
#endif

// Fast SIMD-optimized CSV row parsing
static int csv_parse_row_simd(const char* line, size_t len, csv_row_t* row) {
    // Find comma positions using SIMD
    int comma_positions[8];  // We expect exactly 6 commas for 7 fields
    int comma_count = find_commas_simd(line, len, comma_positions, 8);
    
    if (comma_count != 6) {
        return -1;  // Invalid number of fields
    }
    
    // Parse fields using comma positions
    const char* field_starts[7];
    int field_lengths[7];
    
    // First field starts at beginning
    field_starts[0] = line;
    field_lengths[0] = comma_positions[0];
    
    // Middle fields
    for (int i = 1; i < 6; i++) {
        field_starts[i] = line + comma_positions[i-1] + 1;
        field_lengths[i] = comma_positions[i] - comma_positions[i-1] - 1;
    }
    
    // Last field
    field_starts[6] = line + comma_positions[5] + 1;
    field_lengths[6] = len - comma_positions[5] - 1;
    
    // Parse each field directly without string copying
    char temp_buf[32];
    
#if AVX512_ENABLED
    // Use AVX-512 optimized parsing when available
    
    // date
    if (field_lengths[0] >= 32) return -1;
    uint32_t date_val;
    if (parse_int32_avx512(field_starts[0], field_lengths[0], (int32_t*)&date_val) == 0) {
        row->date = date_val;
    } else {
        fast_memcpy_avx512(temp_buf, field_starts[0], field_lengths[0]);
        temp_buf[field_lengths[0]] = '\0';
        if (parse_uint32(temp_buf, &row->date) < 0) return -1;
    }
    
    // time
    if (field_lengths[1] >= 32) return -1;
    uint16_t time_val;
    if (parse_int32_avx512(field_starts[1], field_lengths[1], (int32_t*)&time_val) == 0 && time_val <= UINT16_MAX) {
        row->time = time_val;
    } else {
        fast_memcpy_avx512(temp_buf, field_starts[1], field_lengths[1]);
        temp_buf[field_lengths[1]] = '\0';
        if (parse_uint16(temp_buf, &row->time) < 0) return -1;
    }
    
    // area (mesh ID)
    if (field_lengths[2] >= 32) return -1;
    if (parse_uint64_avx512(field_starts[2], field_lengths[2], &row->area) < 0) {
        fast_memcpy_avx512(temp_buf, field_starts[2], field_lengths[2]);
        temp_buf[field_lengths[2]] = '\0';
        if (parse_uint64(temp_buf, &row->area) < 0) return -1;
    }
    
    // residence
    if (field_lengths[3] >= 32) return -1;
    if (parse_int32_avx512(field_starts[3], field_lengths[3], &row->residence) < 0) {
        fast_memcpy_avx512(temp_buf, field_starts[3], field_lengths[3]);
        temp_buf[field_lengths[3]] = '\0';
        if (parse_int32(temp_buf, &row->residence) < 0) return -1;
    }
    
    // age
    if (field_lengths[4] >= 32) return -1;
    if (parse_int32_avx512(field_starts[4], field_lengths[4], &row->age) < 0) {
        fast_memcpy_avx512(temp_buf, field_starts[4], field_lengths[4]);
        temp_buf[field_lengths[4]] = '\0';
        if (parse_int32(temp_buf, &row->age) < 0) return -1;
    }
    
    // gender
    if (field_lengths[5] >= 32) return -1;
    if (parse_int32_avx512(field_starts[5], field_lengths[5], &row->gender) < 0) {
        fast_memcpy_avx512(temp_buf, field_starts[5], field_lengths[5]);
        temp_buf[field_lengths[5]] = '\0';
        if (parse_int32(temp_buf, &row->gender) < 0) return -1;
    }
    
    // population
    if (field_lengths[6] >= 32) return -1;
    if (parse_int32_avx512(field_starts[6], field_lengths[6], &row->population) < 0) {
        fast_memcpy_avx512(temp_buf, field_starts[6], field_lengths[6]);
        temp_buf[field_lengths[6]] = '\0';
        if (parse_int32(temp_buf, &row->population) < 0) return -1;
    }
    
#else
    // Standard parsing
    
    // date
    if (field_lengths[0] >= 32) return -1;
    memcpy(temp_buf, field_starts[0], field_lengths[0]);
    temp_buf[field_lengths[0]] = '\0';
    if (parse_uint32(temp_buf, &row->date) < 0) return -1;
    
    // time
    if (field_lengths[1] >= 32) return -1;
    memcpy(temp_buf, field_starts[1], field_lengths[1]);
    temp_buf[field_lengths[1]] = '\0';
    if (parse_uint16(temp_buf, &row->time) < 0) return -1;
    
    // area (mesh ID)
    if (field_lengths[2] >= 32) return -1;
    memcpy(temp_buf, field_starts[2], field_lengths[2]);
    temp_buf[field_lengths[2]] = '\0';
    if (parse_uint64(temp_buf, &row->area) < 0) return -1;
    
    // residence
    if (field_lengths[3] >= 32) return -1;
    memcpy(temp_buf, field_starts[3], field_lengths[3]);
    temp_buf[field_lengths[3]] = '\0';
    if (parse_int32(temp_buf, &row->residence) < 0) return -1;
    
    // age
    if (field_lengths[4] >= 32) return -1;
    memcpy(temp_buf, field_starts[4], field_lengths[4]);
    temp_buf[field_lengths[4]] = '\0';
    if (parse_int32(temp_buf, &row->age) < 0) return -1;
    
    // gender
    if (field_lengths[5] >= 32) return -1;
    memcpy(temp_buf, field_starts[5], field_lengths[5]);
    temp_buf[field_lengths[5]] = '\0';
    if (parse_int32(temp_buf, &row->gender) < 0) return -1;
    
    // population
    if (field_lengths[6] >= 32) return -1;
    memcpy(temp_buf, field_starts[6], field_lengths[6]);
    temp_buf[field_lengths[6]] = '\0';
    if (parse_int32(temp_buf, &row->population) < 0) return -1;
#endif
    
    return 0;
}
#endif

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
    
#if AVX512_ENABLED
    // AVX-512 optimized string comparison for header
    size_t expected_len = strlen(expected);
    size_t buffer_len = strlen(reader->buffer);
    
    if (expected_len == buffer_len && expected_len <= 64) {
        // Load both strings into AVX-512 registers
        __m512i header_data = _mm512_loadu_si512((const __m512i*)reader->buffer);
        __m512i expected_data = _mm512_loadu_si512((const __m512i*)expected);
        
        // Create mask for valid length comparison
        __mmask64 len_mask = (1ULL << expected_len) - 1;
        
        // Compare characters
        __mmask64 cmp_mask = _mm512_cmpeq_epi8_mask(header_data, expected_data);
        
        // Check if all characters within the string length match
        if ((cmp_mask & len_mask) != len_mask) {
            return -1;
        }
    } else if (strcmp(reader->buffer, expected) != 0) {
        return -1;
    }
#else
    if (strcmp(reader->buffer, expected) != 0) {
        return -1;
    }
#endif
    
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
        len--;
    }
    
#if SIMD_ENABLED
    // Try SIMD-optimized parsing first
    if (csv_parse_row_simd(reader->buffer, len, row) == 0) {
        return 0;  // Success with SIMD
    }
    // Fall back to standard parsing if SIMD fails
#endif
    
    // Standard parsing using strtok_r (fallback)
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

int csv_is_simd_enabled(void) {
#if SIMD_ENABLED
    return 1;
#else
    return 0;
#endif
}

int csv_is_avx512_enabled(void) {
#if AVX512_ENABLED
    return 1;
#else
    return 0;
#endif
}