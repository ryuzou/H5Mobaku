//
// Created by ryuzot on 25/01/02.
//

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include "meshid_ops.h"

typedef struct {
    const char* input_time_str;
    int expected_return_value;
    const char* test_case_name; // テストケース名を追加
} TestCase;

int main() {
    printf("Mesh ID list size: %zu\n", meshid_list_size);

    for (size_t i = 0; i < meshid_list_size && i < 10; ++i) {
        //printf("Mesh ID[%zu]: %d\n", i, meshid_list[i]);
    }

    // 検索準備
    cmph_t *hash = prepare_search();
    if (!hash) {
        return 1; // 検索準備に失敗した場合は終了
    }

    // 乱数生成の初期化
    srand((unsigned int)time(nullptr));

    uint32_t *keys = malloc(meshid_list_size * sizeof(uint32_t));
    for (int i = 0; i < meshid_list_size; i++) {
        keys[i] = meshid_list[i];
    }
    printf("Total length of meshid: %lu\n",meshid_list_size);

    // 検索時間計測
    clock_t start_time = clock();
    for (int i = 0; i < meshid_list_size; i++) {
        uint32_t id = search_id(hash, keys[i]);
        assert(meshid_list[id] == keys[i]); // 検索結果が期待されるメッシュIDと一致することを確認
    }
    clock_t end_time = clock();

    // 結果を表示
    double time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    printf("Time taken for %lu searches: %f seconds\n", meshid_list_size, time_taken);

    // メモリ解放
    free(keys);
    cmph_destroy(hash);

    TestCase test_cases[] = {
        {"2016-01-01 00:00:00", 0, "基準時刻と同一"},
        {"2016-01-01 01:00:00", 1, "1時間後"},
        {"2015-12-31 23:00:00", -1, "2時間前 (前日)"},
        {"2016-01-02 00:00:00", 24, "1日後"},
        {"2015-12-31 00:00:00", -1, "1日前"},
        {"invalid time string", -1, "不正な日付文字列"},
        {"2016-01-01 25:00:0", -1, "不正な日付文字列"},
        {"2016-01-01 -1:00:00", -1, "不正な日付文字列"},
        {"2016/01/01 01:00:00", -1, "不正な日付文字列(区切り文字不正)"},
        {"2024-06-16 23:00:00", 74160-1, "最後"}
    };

    int num_test_cases = sizeof(test_cases) / sizeof(TestCase);

    for (int i = 0; i < num_test_cases; ++i) {
        int actual_return_value = get_time_index_mobaku_datetime((char*)test_cases[i].input_time_str);
        // assertで結果を検証
        assert(actual_return_value == test_cases[i].expected_return_value);
    }
    printf("Datetime index transition test passed\n");

    start_time = clock();
    int uint_list[] = {362335691,362335692,362335693,362335694,362335791,362335792,362335793,362335794,362335891,362335892,362335893,362335894,362335991,362335992,362335993,362335994};
    int num_elements = sizeof(uint_list) / sizeof(int);
    cmph_t *local_hash = create_local_mph_from_int(uint_list, num_elements);
    assert(local_hash != nullptr);
    for (size_t i = 0; i < num_elements; ++i) {
        int key = uint_list[i];
        int index = find_local_id(local_hash, key);
        assert(index == i);
        printf("my_list[%zu] (%d) index: %d\n", i, key, index);
    }
    cmph_destroy(local_hash);
    end_time = clock();
    time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    printf("Time taken for little hash search %f seconds\n", time_taken);
    printf("Little local hash test passed\n");

    printf("All tests passed!\n");
    return 0;
}