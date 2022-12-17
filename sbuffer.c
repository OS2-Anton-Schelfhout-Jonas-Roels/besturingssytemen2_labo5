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
    //pthread_mutex_t mutex;
    pthread_rwlock_t rwlock;
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
    //ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_init(&buffer->rwlock, NULL) == 0);        //creatie readwrite lock
    //buffer->rwlock = PTHREAD_RWLOCK_INITIALIZER;
    //printf("Creation of ReadWritelock");
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
    //ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    printf("Destrucion of rwlock");
    ASSERT_ELSE_PERROR(pthread_rwlock_destroy(&buffer->rwlock) == 0);

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
    //ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0); //wordt enkel uitgevoerd door readers, writer checkt nooit al buffer leeg is
    bool res = buffer->head == NULL;
    //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return res;
}

bool sbuffer_is_closed(sbuffer_t* buffer) {
    assert(buffer);
    //ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);    //checkt als er geen writers bezig zijn of staan te wachten, indien neen, gaat crit sec binnen
    bool res = buffer->closed;
    //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //unlock rwlock
    return res;
}

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {    //writer steekt iets in buffer
    assert(buffer && data);
    if (sbuffer_is_closed(buffer))
        return SBUFFER_FAILURE;

    // create new node
    sbuffer_node_t* node = create_node(data);
    assert(node->prev == NULL);

    //ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);    //accure writer lock
    bool wasEmpty = buffer->head == NULL;
    // insert it
    if (buffer->head != NULL)
        buffer->head->prev = node;
    buffer->head = node;
    
    if (buffer->tail == NULL) {
        buffer->tail = node;
    }        
    //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);

    if(wasEmpty) {      //als buffer leeg was, wil dit zeggen dat de readers slapen, dus maak ze wakker
        pthread_cond_signal(&buffer->dataManagerCondition);
        pthread_cond_signal(&buffer->storageManagerCondition);
    }

    return SBUFFER_SUCCESS;
}

sensor_data_t sbuffer_remove_last(sbuffer_t* buffer) {
    assert(buffer);
    //ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
    // if there are no elements in the buffer we check if the buffer is closed, if that is the case we return a new sensor data struct with value -infinite
    // else we wait untill there are new elements in the buffer
    if(buffer->head == NULL) {
        if(buffer->closed) {
            //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
            ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
            sensor_data_t data;
            data.value =  -INFINITY;
            return data;
        }
        //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        while(buffer->head == NULL && !buffer->closed) {    //als buffer leeg is moeten reader threads slapen tot writer er terug iets insteekt
            if(pthread_self() == buffer->dataManager) {
                ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //laat read lock los terwijl sleep
                // printf("Datamanager %ul: sleeping (Current tid: %ul) \n", buffer->dataManager, pthread_self());
                ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->dataManagerMutex) == 0);
                ASSERT_ELSE_PERROR(pthread_cond_wait(&(buffer->dataManagerCondition), &(buffer->dataManagerMutex)) == 0);   
                ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->dataManagerMutex) == 0);
                ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);    //neem read lock terug na sleem
            }  
            else if(pthread_self() == buffer->storageManager) {
                ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //laat read lock los terwijl sleep
                // printf("Storagemanager %ul: sleeping (Current tid: %ul) \n", buffer->dataManager, pthread_self());
                ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->storageManagerMutex) == 0);
                ASSERT_ELSE_PERROR(pthread_cond_wait(&(buffer->storageManagerCondition), &(buffer->storageManagerMutex)) == 0);   
                ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->storageManagerMutex) == 0);

                ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);    //neem read lock terug na sleem
            } 
            else {
                ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //laat read lock los voor return
                sensor_data_t data;
                data.value =  -INFINITY;
                return data;
            }
        }
        // printf("Thread %ul: wakes \n", pthread_self());  
    }
    
    assert(buffer->head != NULL);

    sbuffer_node_t* removed_node = buffer->tail;

    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);

    // if there is no set reader set to this thread and return data
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);    //neem writelock om readby te zetten, moet voor if om dataraces te vermijden
    if(!removed_node->readBy){
        removed_node->readBy = pthread_self();
        //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //unlocken voor return!
        return removed_node->data;
    } 
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //voor moest if niet zijn uitgevoerd
    // printf("READBY: %lu \n" , removed_node->readBy);
    // if the node is already read by another node delete the node and return data
    if(removed_node->readBy != pthread_self()) {
        assert(removed_node != NULL);
        ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
        if (removed_node == buffer->head) {
            buffer->head = NULL;
            assert(removed_node == buffer->tail);
        }
        buffer->tail = removed_node->prev;
        //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);

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
        ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);    //neem readlock om te zien als buffer leeg is
        if(buffer->closed) {
            ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //laat read lock los voor return
            //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
            sensor_data_t data;
            data.value =  -INFINITY;

            return data;
        }
        //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        if(pthread_self() == buffer->dataManager) {
            ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
            pthread_cond_signal(&buffer->storageManagerCondition);
            // printf("Datamanager %ul: sleeping \n", pthread_self());
            pthread_mutex_lock(&buffer->dataManagerMutex);
            pthread_cond_wait(&(buffer->dataManagerCondition), &(buffer->dataManagerMutex));   
            pthread_mutex_unlock(&buffer->dataManagerMutex);
        }  
        else if(pthread_self() == buffer->storageManager) {
            ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
            // printf("Storagemanager %ul: sleeping \n", pthread_self());
            pthread_mutex_lock(&buffer->storageManagerMutex);
            pthread_cond_wait(&(buffer->storageManagerCondition), &(buffer->storageManagerMutex));   
            pthread_mutex_unlock(&buffer->storageManagerMutex);
        } 
        // printf("Thread %ul: wakes \n", pthread_self()); 

        return sbuffer_remove_last(buffer);
    }

    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);    //neem writelock om readby en previous_node te zetten, moet voor if om dataraces te vermijden
    if(!removed_node->readBy){
        removed_node->readBy = pthread_self();
        //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);    //unlocken voor return!
        return removed_node->data;
    } 

    if (removed_node == buffer->head) {
        buffer->head = previous_node;
        // assert(removed_node == buffer->tail);
    }
    previous_node->prev = removed_node->prev;
    //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);

    sensor_data_t ret = removed_node->data;
    free(removed_node);
    return ret;

}

void sbuffer_close(sbuffer_t* buffer) {
    assert(buffer);
    //ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);    //write in buffer
    buffer->closed = true;  
    //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
}


void setManagers(sbuffer_t* buffer, unsigned long datamgr, unsigned long storagemgr) {
    // printf("Datamanger: %ul, StorageManger; %ul \n", datamgr, storagemgr);
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    buffer->dataManager = datamgr;
    buffer->storageManager = storagemgr;
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    // printf("Datamanger: %ul, StorageManger; %ul \n", buffer->dataManager, buffer->storageManager);
}
