/**
 * \author Mathieu Erbas
 */

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "sbuffer.h"

#include "config.h"
#include <math.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

typedef struct sbuffer_node {
    struct sbuffer_node* prev;
    sensor_data_t data;
    pthread_t readBy;
} sbuffer_node_t;

struct sbuffer {
    sbuffer_node_t* head;
    sbuffer_node_t* tail;
    bool closed;
    pthread_mutex_t mutex;
};

static sbuffer_node_t* create_node(const sensor_data_t* data) {
    sbuffer_node_t* node = malloc(sizeof(*node));
    *node = (sbuffer_node_t){
        .data = *data,
        .prev = NULL,
    };
    return node;
}

sbuffer_t* sbuffer_create() {
    sbuffer_t* buffer = malloc(sizeof(sbuffer_t));
    // should never fail due to optimistic memory allocation
    assert(buffer != NULL);

    buffer->head = NULL;
    buffer->tail = NULL;
    buffer->closed = false;
    ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);

    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    free(buffer);
}

void sbuffer_lock(sbuffer_t* buffer) {
    // assert(buffer);
    // ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
}
void sbuffer_unlock(sbuffer_t* buffer) {
    // assert(buffer);
    // ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}

bool sbuffer_is_empty(sbuffer_t* buffer) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    bool res = buffer->head == NULL;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return res;
}

bool sbuffer_is_closed(sbuffer_t* buffer) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    bool res = buffer->closed;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return res;
}

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {
    assert(buffer && data);
    if (sbuffer_is_closed(buffer))
        return SBUFFER_FAILURE;

    // create new node
    sbuffer_node_t* node = create_node(data);
    assert(node->prev == NULL);

    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    // insert it
    if (buffer->head != NULL)
        buffer->head->prev = node;
    buffer->head = node;
    if (buffer->tail == NULL)
        buffer->tail = node;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return SBUFFER_SUCCESS;
}

sensor_data_t sbuffer_remove_last(sbuffer_t* buffer) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    if(buffer->head == NULL) {
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        sensor_data_t data;
        data.value =  -INFINITY;
        return data;
    }
    
    assert(buffer->head != NULL);

    sbuffer_node_t* removed_node = buffer->tail;

    // if there is no set reader set to this thread and return data
    if(!removed_node->readBy){
        removed_node->readBy = pthread_self();
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        return removed_node->data;
    } 
    // printf("READBY: %lu \n" , removed_node->readBy);
    // if the node is already read by another node delete the node and return data
    if(removed_node->readBy != pthread_self()) {
        assert(removed_node != NULL);
        if (removed_node == buffer->head) {
            buffer->head = NULL;
            assert(removed_node == buffer->tail);
        }
        buffer->tail = removed_node->prev;
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);

        sensor_data_t ret = removed_node->data;
        free(removed_node);
        return ret;
    }
    sbuffer_node_t* previous_node = removed_node;
    removed_node = removed_node->prev;
    while(removed_node != NULL && removed_node->readBy == pthread_self()) {
        previous_node = removed_node;
        removed_node = removed_node->prev;
    }

    if(removed_node == NULL) {
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        sensor_data_t data;
        data.value =  -INFINITY;
        return data;
    }

    if(!removed_node->readBy){
        removed_node->readBy = pthread_self();
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        return removed_node->data;
    } 

    if (removed_node == buffer->head) {
        buffer->head = previous_node;
        // assert(removed_node == buffer->tail);
    }
    previous_node->prev = removed_node->prev;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);

    sensor_data_t ret = removed_node->data;
    free(removed_node);
    return ret;



}

void sbuffer_close(sbuffer_t* buffer) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    buffer->closed = true;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}
