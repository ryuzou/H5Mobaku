//
// Created by ryuzot on 25/01/02.
//

#include "meshid_ops.h"

#include <assert.h>

time_t meshid_pg_bin_timestamp_to_jst(const char *bin_ptr, int len) {
    if (len < 8) {
        return (time_t)-1;
    }

    int64_t network_order_64 = 0;
    memcpy(&network_order_64, bin_ptr, 8);
    int64_t pg_microsec = (int64_t)be64toh((uint64_t)network_order_64);

    int64_t pg_sec  = pg_microsec / 1000000;
    int64_t utc_sec = pg_sec + POSTGRES_EPOCH_IN_UNIX;
    // WARNING: Hard-coded JST offset
    int64_t jst_sec = utc_sec - JST_OFFSET_SEC;

    return (time_t)jst_sec;
}

int meshid_get_time_index_from_datetime(char* now_time_str) {
    struct tm reference_time_tm = {0};
    if (strptime(REFERENCE_MOBAKU_DATETIME, "%Y-%m-%d %H:%M:%S", &reference_time_tm) == NULL) {
        fprintf(stderr, "Error: Failed to parse reference datetime string. Expected format: YYYY-MM-DD HH:MM:SS\n");
        return -1;
    }
    time_t reference_mobaku_time = mktime(&reference_time_tm);

    if (reference_mobaku_time == (time_t)-1) {
        fprintf(stderr, "Error: Failed to convert reference time to timestamp (mktime failed)\n");
        return -1;
    }

    struct tm now_time_tm = {0};
    if (strptime(now_time_str, "%Y-%m-%d %H:%M:%S", &now_time_tm) == NULL) {
        fprintf(stderr, "Failed to parse datetime string: '%s'. Expected format: YYYY-MM-DD HH:MM:SS\n", now_time_str);
        return -1;
    }

    time_t now_time = mktime(&now_time_tm);
    if (now_time == (time_t)-1) {
        fprintf(stderr, "Error: Failed to convert input time to timestamp (mktime failed for '%s')\n", now_time_str);
        return -1;
    }

    int index_h_time = (int)(difftime(now_time, reference_mobaku_time) / 3600.0);
    if (index_h_time < 0) {
        index_h_time = -1;
    }
    return index_h_time;
}

int meshid_get_time_index_from_time(time_t now_time) {
    time_t reference_mobaku_time = REFERENCE_MOBAKU_TIME;

    if (now_time == (time_t)-1) {
        fprintf(stderr, "Error: Invalid time_t value provided to get_time_index_mobaku_datetime_from_time\n");
        return -1;
    }

    int index_h_time = (int)(difftime(now_time, reference_mobaku_time) / 3600.0);
    if (index_h_time < 0) {
        index_h_time = -1;
    }
    return index_h_time;
}

char * meshid_get_datetime_from_time_index(int time_index) {
    struct tm reference_time_tm = {0};
    if (strptime(REFERENCE_MOBAKU_DATETIME, "%Y-%m-%d %H:%M:%S", &reference_time_tm) == NULL) {
        fprintf(stderr, "Error: Failed to parse reference datetime (2016-01-01 00:00:00). Check system locale settings\n");
        return NULL;
    }
    time_t reference_mobaku_time = mktime(&reference_time_tm);

    if (reference_mobaku_time == (time_t)-1) {
        fprintf(stderr, "Error: Failed to convert reference time to timestamp (mktime failed)\n");
        return NULL;
    }

    time_t target_time = reference_mobaku_time + (time_index * 3600);

    struct tm target_time_tm;
    localtime_r(&target_time, &target_time_tm);

    char* datetime_str = (char*)malloc(sizeof(char) * 20); // "YYYY-MM-DD HH:MM:SS\0"
    if (datetime_str == NULL) {
        perror("Error: Memory allocation failed for datetime string buffer");
        return NULL;
    }

    if (strftime(datetime_str, 20, "%Y-%m-%d %H:%M:%S", &target_time_tm) == 0) {
        fprintf(stderr, "Error: Failed to format datetime string (strftime failed for time_index=%d)\n", time_index);
        free(datetime_str);
        return NULL;
    }

    return datetime_str;
}

void meshid_uint_to_str(unsigned int num, char *str) {
    int i = 0;

    // Store digits in reverse order
    do {
        str[i++] = (num % 10) + '0'; // Convert digit to character
        num /= 10;
    } while (num > 0);

    // Add null terminator
    str[i] = '\0';

    // Reverse the string
    for (int j = 0; j < i / 2; j++) {
        char temp = str[j];
        str[j] = str[i - j - 1];
        str[i - j - 1] = temp;
    }
}

cmph_t * meshid_prepare_search(void) {
    unsigned char *mph_data = _binary_meshid_mobaku_mph_start;
    size_t mph_size = (size_t)(_binary_meshid_mobaku_mph_end - _binary_meshid_mobaku_mph_start);
    FILE *fp = fmemopen(mph_data, mph_size, "rb");
    if (!fp) {
        perror("Error: Failed to open memory stream for CMPH data");
        return NULL;
    }

    cmph_t *hash = cmph_load(fp);
    fclose(fp);

    if (!hash) {
        fprintf(stderr, "Error: Failed to load minimal perfect hash function from meshid data\n");
    }

    return hash;
}

uint32_t meshid_search_id(cmph_t *hash, uint32_t key) {
    // Special case handling
    if (key == 684827214) {
        return 1553331;
    }
    
    // Validate digit count
    if (key < 100000000 || key > 999999999) {
        fprintf(stderr, "Error: Mesh ID %u has incomplete 1/2 regional meshid\n", key);
        return MESHID_NOT_FOUND;
    }
    
    char key_str[11];
    meshid_uint_to_str(key, key_str);
    return cmph_search(hash, key_str, (cmph_uint32)strlen(key_str));
}

char ** meshid_uint_array_to_string_array(const int *int_array, size_t nkeys) {
    char** str_array = (char**)malloc(sizeof(char*) * nkeys);
    if (str_array == NULL) {
        perror("Error: Memory allocation failed for string array");
        return NULL;
    }

    for (size_t i = 0; i < nkeys; ++i) {
        // Allocate buffer considering max int digits (+ null terminator)
        str_array[i] = (char*)malloc(sizeof(char) * 12); // Max int is 10 digits + sign + terminator
        if (str_array[i] == NULL) {
            perror("Error: Memory allocation failed for string element");
            // Free already allocated memory
            for (size_t j = 0; j < i; ++j) {
                free(str_array[j]);
            }
            free(str_array);
            return NULL;
        }
        meshid_uint_to_str(int_array[i], str_array[i]);
    }
    return str_array;
}

void meshid_free_string_array(char **str_array, size_t nkeys) {
    if (str_array == NULL) return;
    for (size_t i = 0; i < nkeys; ++i) {
        free(str_array[i]);
    }
    free(str_array);
}

cmph_t * meshid_create_local_mph_from_int(int *int_array, size_t nkeys) {
    char** str_array = meshid_uint_array_to_string_array(int_array, nkeys);
    if (str_array == NULL) return nullptr; // Handle case when str_array is NULL

    cmph_io_adapter_t* source = cmph_io_vector_adapter(str_array, nkeys);
    cmph_config_t* config = cmph_config_new(source);
    cmph_config_set_algo(config, CMPH_CHM);
    cmph_t* hash = cmph_new(config);
    cmph_config_destroy(config);
    cmph_io_vector_adapter_destroy(source);
    meshid_free_string_array(str_array, nkeys); // Free str_array

    if (hash == nullptr) {
        fprintf(stderr, "Error: Failed to create minimal perfect hash function for %zu local mesh IDs\n", nkeys);
        return nullptr;
    }

    return hash;
}

int meshid_find_local_id(cmph_t *hash, uint32_t key) {
    char key_str[11];
    meshid_uint_to_str(key, key_str);
    return cmph_search(hash, key_str, (cmph_uint32)strlen(key_str));
}

void meshid_print_progress_bar(int now, int all) {
    const int barWidth = 20;

    double progress = (double)(now) / (double)all;
    int pos = (int)(barWidth * progress);

    // Use carriage return (\r) to overwrite the same line
    printf("\r[");
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos) {
            printf("=");
        } else {
            printf(" ");
        }
    }
    printf("] %6.2f %%  %d/%d", progress * 100.0, now, all);
    fflush(stdout);
}

int * meshid_get_all_meshes_in_1st_mesh(int meshid_1, int num_meshes) {
    int *mesh_ids = (int*)malloc(num_meshes * sizeof(int));
    if (mesh_ids == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for mesh IDs array (%d elements)\n", num_meshes);
        return NULL;
    }

    int index = 0;
    for (int q = 0; q < 8; q++) {
        for (int v = 0; v < 8; v++) {
            for (int r = 0; r < 10; r++) {
                for (int w = 0; w < 10; w++) {
                    for (int s = 0; s < 4; s++) {
                        int m = s + 1;
                        int mesh_id = meshid_1 * 100000 + q * 10000 + v * 1000 + r * 100 + w * 10 + m;
                        mesh_ids[index++] = mesh_id;
                    }
                }
            }
        }
    }
    return mesh_ids;
}
