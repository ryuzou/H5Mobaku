//
// Created by ryuzot on 25/01/08.
//

#include "fifioq.h"

void init_queue(FIFOQueue *q) {
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);
    sem_init(&q->full, 0, QUEUE_SIZE);
    sem_init(&q->empty, 0, 0);
}

void enqueue(FIFOQueue *q, void *data) {
    sem_wait(&q->full);
    pthread_mutex_lock(&q->mutex);
    q->queue[q->tail] = data;
    q->tail = (q->tail + 1) % QUEUE_SIZE;
    q->count++;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->empty);
}

void * dequeue(FIFOQueue *q) {
    sem_wait(&q->empty);
    pthread_mutex_lock(&q->mutex);
    void *data = q->queue[q->head];
    q->head = (q->head + 1) % QUEUE_SIZE;
    q->count--;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->full);
    return data;
}