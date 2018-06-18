#include "queue.h"
#include <errno.h>
#include "csapp.h"
/*
 * Creates and returns an instance of a queue
 * and initializes all locks
 *
 * @return A pointer to a queue on the heap
 */
queue_t *create_queue(void){
    struct queue_t *queue = calloc(1,sizeof(struct queue_t));
    if (queue == NULL){
        return NULL;
    }
    pthread_mutex_t lock = queue -> lock;
    int b = pthread_mutex_init(&lock, NULL);
    if (b == -1){
        return NULL;
    }
    //sem_t items = queue -> items;
    int a = sem_init(&queue->items, 0, 0);
    if (a == -1){
        return NULL;
    }
    queue -> front = NULL;
    queue -> rear = NULL;
    queue -> invalid = false;
    return queue;
}

bool invalidate_queue(queue_t *self, item_destructor_f destroy_function) {
    if (self == NULL){
        errno = EINVAL;
        return false;
    }
    pthread_mutex_lock(&self->lock);
    struct queue_node_t *node = self->front;
    while (node != NULL) {
        destroy_function(node);
        node = node->next;
    }
    self-> invalid = true;
    pthread_mutex_unlock(&self->lock);
    return true;
}

bool enqueue(queue_t *self, void *item) {
    if (self == NULL){
        errno = EINVAL;
        return false;
    }
    else if (self->invalid == true){
        errno = EINVAL;
        return false;
    }
    else if (item == NULL){
        errno = EINVAL;
        return false;
    }
    pthread_mutex_lock(&self->lock);
    struct queue_node_t *node = calloc(1,sizeof(struct queue_node_t));
    node -> item = item;
    if (self -> front == NULL){
        self->front = node;
        self->rear = node;
    }
    else{
        struct queue_node_t *prev = self->rear;
        prev->next = node;
        self-> rear = node;
    }
    //sem_t items = self -> items;
    sem_post(&self->items);
    //self -> items = items;
    pthread_mutex_unlock(&self->lock);
    return true;
}

void *dequeue(queue_t *self) {
    if (self == NULL){
        errno = EINVAL;
        return false;
    }
    else if (self->invalid == true){
        errno = EINVAL;
        return false;
    }

    sem_wait(&self->items);
    //while(ret == -1 && errno == EINTR){
    //printf("%d\n",ret);
        //dequeue(self);
    //}
    pthread_mutex_lock(&self->lock);
    struct queue_node_t *node = self->front;
    if (node == NULL){
        return NULL;
    }
    //self -> items = items;
    void *item = node->item;
    struct queue_node_t *next = node->next;
    self->front = next;
    free(node);
    //sem_t items = self -> items;
    pthread_mutex_unlock(&self->lock);
    return item;
}
