//
// Created by ryuzot on 25/06/04.
//
#ifndef ENV_UTILS_H
#define ENV_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

const char* get_env_value(const char* key, const char* default_value);

#ifdef __cplusplus
}
#endif

#endif // ENV_UTILS_H