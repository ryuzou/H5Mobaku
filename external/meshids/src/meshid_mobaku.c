#include <stdint.h>
#include <stddef.h>

const uint32_t meshid_list[] = {
#include "meshid_1_2rd.csv"
};

const size_t meshid_list_size = sizeof(meshid_list)/sizeof(meshid_list[0]);