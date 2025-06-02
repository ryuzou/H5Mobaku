#include "hdf5_ops.h"
#include <pthread.h>
#include <stdio.h>

#define DATASET_SIZE 100
#define NUM_THREADS 4

typedef struct {
    hdf5_thread_safe_t* hdf5;
    int thread_id;
} thread_data_t;

void* thread_function(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    int write_data[DATASET_SIZE];

    // 書き込むデータを生成
    for (int i = 0; i < DATASET_SIZE; i++) {
        write_data[i] = data->thread_id * DATASET_SIZE + i;
    }

    // データを書き込む
    hdf5_write(data->hdf5, write_data, data->thread_id * DATASET_SIZE, DATASET_SIZE);
    return NULL;
}

int main() {
    hdf5_thread_safe_t* hdf5 = hdf5_create("example.h5", "MyDataset", DATASET_SIZE * NUM_THREADS);

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].hdf5 = hdf5;
        thread_data[i].thread_id = i;
        pthread_create(&threads[i], NULL, thread_function, &thread_data[i]);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    hdf5_close(hdf5);
    printf("データの書き込みが完了しました。\n");
    return 0;
}