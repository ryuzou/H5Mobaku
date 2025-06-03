//
// Created by ryuzot on 25/06/04.
//
#include "env_utils.h"

static char env_buffer[256];

const char* get_env_value(const char* key, const char* default_value) {
    const char* env_val = getenv(key);
    if (env_val != NULL) {
        return env_val;
    }
    
    FILE* env_file = fopen(".env", "r");
    if (env_file == NULL) {
        return default_value;
    }
    
    char line[256];
    while (fgets(line, sizeof(line), env_file)) {
        line[strcspn(line, "\n")] = 0;
        
        if (line[0] == '#' || line[0] == '\0') {
            continue;
        }
        
        char* equals = strchr(line, '=');
        if (equals == NULL) {
            continue;
        }
        
        *equals = '\0';
        char* env_key = line;
        char* env_value = equals + 1;
        
        if (strcmp(env_key, key) == 0) {
            fclose(env_file);
            strncpy(env_buffer, env_value, sizeof(env_buffer) - 1);
            env_buffer[sizeof(env_buffer) - 1] = '\0';
            return env_buffer;
        }
    }
    
    fclose(env_file);
    return default_value;
}