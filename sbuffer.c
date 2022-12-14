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
    pthread_cond_t dataManagerCondition;
    pthread_cond_t storageManagerCondition;
    pthread_mutex_t dataManagerMutex;
    pthread_mutex_t storageManagerMutex;
    unsigned long dataManager;
    unsigned long storageManager;
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
    buffer->dataManager = 0;
    buffer->storageManager = 0;

    pthread_cond_init(&buffer->dataManagerCondition, NULL);
    pthread_cond_init(&buffer->storageManagerCondition, NULL);
    pthread_mutex_init(&buffer->dataManagerMutex, NULL);
    pthread_mutex_init(&buffer->storageManagerMutex, NULL);
    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);

    pthread_cond_destroy(&buffer->dataManagerCondition);
    pthread_cond_destroy(&buffer->storageManagerCondition);
    pthread_mutex_destroy(&buffer->dataManagerMutex);
    pthread_mutex_destroy(&buffer->storageManagerMutex);
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
    bool wasEmpty = buffer->head == NULL;
    // insert it
    if (buffer->head != NULL)
        buffer->head->prev = node;
    buffer->head = node;
    
    if (buffer->tail == NULL) {
        buffer->tail = node;
    }        
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);

    if(wasEmpty) {
        pthread_cond_signal(&buffer->dataManagerCondition);
    }

    return SBUFFER_SUCCESS;
}

sensor_data_t sbuffer_remove_last(sbuffer_t* buffer) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    // if there are no elements in the buffer we check if the buffer is closed, if that is the case we return a new sensor data struct with value -infinite
    // else we wait untill there are new elements in the buffer
    if(buffer->head == NULL) {
        if(buffer->closed) {
            ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
            sensor_data_t data;
            data.value =  -INFINITY;
            return data;
        }
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        while(buffer->head == NULL && !buffer->closed) {
            if(pthread_self() == buffer->dataManager) {
                // printf("Datamanager %ul: sleeping (Current tid: %ul) \n", buffer->dataManager, pthread_self());
                ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->dataManagerMutex) == 0);
                ASSERT_ELSE_PERROR(pthread_cond_wait(&(buffer->dataManagerCondition), &(buffer->dataManagerMutex)) == 0);   
                ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->dataManagerMutex) == 0);
            }  
            else if(pthread_self() == buffer->storageManager) {
                // printf("Storagemanager %ul: sleeping (Current tid: %ul) \n", buffer->dataManager, pthread_self());
                ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->storageManagerMutex) == 0);
                ASSERT_ELSE_PERROR(pthread_cond_wait(&(buffer->storageManagerCondition), &(buffer->storageManagerMutex)) == 0);   
                ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->storageManagerMutex) == 0);
            } 
            else {
                sensor_data_t data;
                data.value =  -INFINITY;
                return data;
            }
        }
        // printf("Thread %ul: wakes \n", pthread_self());  
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
        if(buffer->closed) {
            ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
            sensor_data_t data;
            data.value =  -INFINITY;
            return data;
        }
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        if(pthread_self() == buffer->dataManager) {
            pthread_cond_signal(&buffer->storageManagerCondition);
            // printf("Datamanager %ul: sleeping \n", pthread_self());
            pthread_mutex_lock(&buffer->dataManagerMutex);
            pthread_cond_wait(&(buffer->dataManagerCondition), &(buffer->dataManagerMutex));   
            pthread_mutex_unlock(&buffer->dataManagerMutex);
        }  
        else if(pthread_self() == buffer->storageManager) {
            // printf("Storagemanager %ul: sleeping \n", pthread_self());
            pthread_mutex_lock(&buffer->storageManagerMutex);
            pthread_cond_wait(&(buffer->storageManagerCondition), &(buffer->storageManagerMutex));   
            pthread_mutex_unlock(&buffer->storageManagerMutex);
        } 
        // printf("Thread %ul: wakes \n", pthread_self()); 

        return sbuffer_remove_last(buffer);
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


void setManagers(sbuffer_t* buffer, unsigned long datamgr, unsigned long storagemgr) {
    // printf("Datamanger: %ul, StorageManger; %ul \n", datamgr, storagemgr);
    buffer->dataManager = datamgr;
    buffer->storageManager = storagemgr;
    // printf("Datamanger: %ul, StorageManger; %ul \n", buffer->dataManager, buffer->storageManager);
}