//
// Created by ryuzot on 25/01/08.
//

#ifndef FIFIOQ_H
#define FIFIOQ_H

#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>

#define QUEUE_SIZE 1024

typedef struct {
    void *queue[QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    sem_t full;     // number of items in the queue. increase when queue is enqueued, decrease when dequeued.
    sem_t empty;    // number of items in the queue. increase when queue is dequeued, decrease when enqueued.
} FIFOQueue;


void init_queue(FIFOQueue *q);

void enqueue(FIFOQueue *q, void *data);

void *dequeue(FIFOQueue *q);

// Non-blocking dequeue with timeout (returns NULL on timeout)
void *dequeue_timed(FIFOQueue *q, int timeout_ms);


#endif //FIFIOQ_H