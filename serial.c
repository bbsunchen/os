 /*******************************************************************************
 * File:     serial.c
 * Serializer. CSE 511 Project
 *
 * All parallel code are summed up in these two words: sem_wait and sem_post!
 * 
 * Copyright (C) Oct.2015 Haibo, Chen
 *
 * This program is not free software: you can not redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, neither version 3 of the License, nor
 * (at your option) any later version.
 * 
 * This program is not distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details.
 * 
 * You will not receive a copy of the GNU General Public License
 * along with this program. 
 *
 * Contact Information:
 * Haibo <haibo@cse.psu.edu>
 * 111 IST, University Park, PA, 16302
 *
 ******************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h> 
#include <assert.h>


#include "serial.h"



void print_queue_stats(queue_t * q){
    qnode_t * head = q->_head;
    int semv = -314159;
    sem_getvalue(&(q->sem), &semv);    
    if (head == NULL)
    {
        printf("empty, sem: %d\n", semv);
        return ;
    }
    while(head != NULL){
        printf("%x->", head->tid);
        head = head->next;
    }

    printf("null, sem: %d\n", semv);
    return ;
}

void print_serial_stats(serial_t * s){
    queues_t * qs = s->queues;
    //printf("I am: %x  ",pthread_self());
    printf("enter queue: ");
    print_queue_stats(qs->enter_queue);
    
    if (s->nwqs >= 2)
    {
        printf("waiting queue1: ");
        print_queue_stats(s->waitqs[0]);
        printf("waiting queue2: ");
        print_queue_stats(s->waitqs[1]);
    } else {
        printf("waiting queue: ");
        if (qs->waiting_queue == NULL)
            printf("NULL\n");
        else
            print_queue_stats(qs->waiting_queue);
    }
    printf("leave crowd queue: ");
    print_queue_stats(qs->leave_crowd_queue);
    return ;    
}

queue_t* queue_t_enqueue(serial_t * s, queue_t * q, int priority)
{
    pthread_mutex_lock(&(q->q_mutex));
    qnode_t *nd = (qnode_t *)malloc(sizeof(qnode_t));
    queue_t * realq = NULL;
    nd->tid  = pthread_self();
    nd->next = NULL;
    nd->priority = priority;
    nd->current_cylinder = priority;
    if (nd->current_cylinder < 0)
        nd->current_cylinder *= -1;
    //printf("\nqueue_t_enqueue node priority: %d\n", priority);
    if (q->count == 0)
    {
        sem_init(&(nd->sem), 0, 1);
        q->_head = nd;
        q->_tail = nd;
        realq = q;
        q->count++;
       // printf("CASE1: RealQueue: %x\n", realq);         
    } // original queue, count = 0
    else {
        sem_init(&(nd->sem), 0, 0);

        qnode_t * curr_nd = q->_head;
        qnode_t * next_nd = curr_nd->next;

        assert (curr_nd != NULL);
        if (nd->priority > curr_nd->priority){
            assert(s->nwqs >= 2); 
            struct queue_t*  nextwq = q;
            if (s->nwqs >= 2) 
            {
                int i = 0;
                for (i = 0; i < s->nwqs ; i++)
                {
                    if (q != s->waitqs[i]){
                        nextwq = s->waitqs[i];
                        break;
                    }
                }

                assert(q != nextwq);
                nd->priority *= -1;
                pthread_mutex_lock(&(nextwq->q_mutex));
                if (nextwq->count == 0)
                {
                    sem_init(&(nd->sem), 0, 1);
                    nextwq->_head = nd;
                    nextwq->_tail = nd;
                } else {
                    sem_init(&(nd->sem), 0, 0);
                    qnode_t * nextwq_curr_nd = nextwq->_head;
                    qnode_t * nextwq_next_nd = nextwq_curr_nd->next;
                    
                    if (nd->priority > nextwq_curr_nd->priority){
                        nd->next = nextwq->_head;
                        nextwq->_head = nd;
                        sem_init(&(nd->next->sem), 0, 0);
                        sem_init(&(nd->sem), 0, 1);
                    } else {
                        while (nextwq_next_nd != NULL && nextwq_next_nd->priority >= nd->priority){
                            nextwq_curr_nd = nextwq_next_nd;
                            nextwq_next_nd = nextwq_next_nd->next;
                        }// curr > nd > next
                        nextwq_curr_nd->next = nd;
                        nd->next = nextwq_next_nd;
                    }
                }
                nextwq->count ++;
                pthread_mutex_unlock(&(nextwq->q_mutex));
            }
            realq = nextwq;            
        } // next queue.
        else{ 
            while (next_nd != NULL && next_nd->priority >= nd->priority){
                curr_nd = next_nd;
                next_nd = next_nd->next;
            }// curr > nd > next
            curr_nd->next = nd;
            nd->next = next_nd;
            realq = q;
            q->count++;
        }  // original queue.

    }//
    
    //printf("enqueue : ");
    //print_serial_stats(gs);
    pthread_mutex_unlock(&(q->q_mutex));

    sem_wait(&(nd->sem));

    return realq;
}

qnode_t * queue_t_dequeue(queue_t * q){
   // int semv = -314159;

    pthread_mutex_lock(&(q->q_mutex));

    if (q->count <= 0) {
    pthread_mutex_unlock(&(q->q_mutex));
        assert(0);//Wrong! No element in the queue.
        return NULL;
    }
    else {
        qnode_t * head = q->_head;
        q->_head = q->_head->next;
        q->count--;
        if (head->next != NULL){ 
            sem_post(&((head->next)->sem));
        }
    //printf("dequeue : ");        
    //print_serial_stats(gs);
    pthread_mutex_unlock(&(q->q_mutex));
        return head;
    }
}

qnode_t * queue_t_head(queue_t * q){
    assert(q->count > 0);
    if (q->count <= 0) {
        return NULL;
    }
    else {
        return q->_head;
    }
}

/* Create a serializer
 * return: a handler to a serializer.
 */
serial_t * Create_Serial(){
    serial_t * serializer = (serial_t *)malloc(sizeof(serial_t));

    //initize serialzier's queue
    serializer->queues = create_queues(serializer, kWAITINGQ1);

    //initize serialzier's crowd
    serializer->rd_crowd = Create_Crowd(serializer);
    serializer->wr_crowd = Create_Crowd(serializer);

    sem_init(&(serializer->sem), 0, 1);
    sem_init(&(serializer->count_sem), 0, 1);

    serializer->direction = kUP;
    serializer->cylinder = 0;

    serializer->waitqs = NULL;
    serializer->nwqs   = 0;

    return serializer;
}

/*
 * ENTER enter_queue;
 */
void Serial_Enter(serial_t * s)
{
    struct queue_t *eq = s->queues->enter_queue;

    eq->enqueue(s, eq, 0); // how to decide priority?

    /*** I am head of Enter Q***/

    /***gain serializer***/
    //sem_wait(&(s->sem));

    eq->dequeue(eq);

    pthread_mutex_lock(&s_mutex);
}



/*
 * In serializer. so only one process is in.
 *
 */
void Serial_Exit(serial_t * s)
{
    pthread_mutex_unlock(&s_mutex);

    leave_crowd(s, 0);//haibo

    pthread_mutex_unlock(&s_mutex);
    leave_serializer(s, (s->queues)->enter_queue); // this time, we need to give semophore to right queue

}

queues_t* create_queues(serial_t * serializer, const int Waiting_Queue_Number){
    queues_t* qs = (queues_t *)malloc(sizeof(queues_t));
    qs->enter_queue = create_queue(serializer, 0);
    qs->leave_crowd_queue = create_queue(serializer, 0);
    qs->waiting_queue = NULL;
    qs->waiting_queues    =  (queue_t **)malloc(Waiting_Queue_Number * sizeof(queue_t *));

    int waitingQId = 0;
    
    for (waitingQId = 0; waitingQId < Waiting_Queue_Number; waitingQId++) {
        (qs->waiting_queues)[waitingQId] = create_queue(serializer, 0);
    }
    return qs;
}



queue_t* create_queue(serial_t * s, int initial_sem)
{

    queue_t* q  = (queue_t *)malloc(sizeof(queue_t));
    /*** assume successfully create queue**/    
    assert(q != NULL); 

    //initize serialzier's queue

    q->_tail     = NULL;
    q->_head     = q->_tail;
    q->enqueue  = &queue_t_enqueue;
    q->dequeue  = &queue_t_dequeue;
    q->head     = &queue_t_head   ;
    q->count    = 0;
    q->s   = s;
    q->isHeadChanged = 0;
    sem_init(&(q->sem), 0, initial_sem);
    return q;
}
    

queue_t* Create_Queue(serial_t * serializer)
{
    static int numOfQueue_ = 0;

    numOfQueue_++;
    queue_t **waitqs = (queue_t **) malloc (numOfQueue_*sizeof(queue_t*));
    int i = 0;
    for (i = 0; i < numOfQueue_-1; i++)
        waitqs[i] = (serializer->waitqs)[i];
    free(serializer->waitqs);


    queue_t* q  = (queue_t *)malloc(sizeof(queue_t));
    /*** assume successfully create queue**/    
    assert(q != NULL); 
    //initize serialzier's queue
    q->_tail     = NULL;
    q->_head     = q->_tail;
    q->enqueue  = &queue_t_enqueue;
    q->dequeue  = &queue_t_dequeue;
    q->head     = &queue_t_head   ;
    q->count    = 0;
    q->isHeadChanged = 0;
    sem_init(&(q->sem), 0, 0);
    waitqs[numOfQueue_-1] = q;
    serializer->waitqs = waitqs;
    serializer->nwqs = numOfQueue_;
    return q;
}

crowd_t * Create_Crowd(serial_t * serializer)
{
    crowd_t* c  = (crowd_t *)malloc(sizeof(crowd_t));
    c->count = 0;
    return c;
}


int Queue_Empty(serial_t *s, queue_t *q)
{   
    if (q->count > 0) return kNOTEMPTY;
    return kEMPTY;
}

int Crowd_Empty(serial_t *s, crowd_t * c)
{
    int count =  -314159;
    pthread_mutex_lock(&(c->c_mutex));
    count = c->count;
    pthread_mutex_unlock(&(c->c_mutex));    
    if (count > 0) return kNOTEMPTY;
    return kEMPTY;
}


/**Enter Waiting Queue**/
void Serial_Enqueue(serial_t * s, queue_t * wq, cond_t (*func)(), int priority)
{

    pthread_mutex_unlock(&s_mutex);

    (s->queues)->waiting_queue = wq;
    /**
    If all queues are empty, send a semophore for waiting queue for this thread.
    **/
    leave_serializer(s, wq);

    queue_t* realwq = wq->enqueue(s, wq, priority);
  
    /***gain right of queue***/
    sem_wait(&(realwq->sem));
    
    pthread_mutex_lock(&waiting_mutex);
    sem_wait(&(s->count_sem));
    while(func() == kNOTEMPTY) {
        usleep(1000);
    }
          
    realwq->dequeue(realwq);

    pthread_mutex_unlock(&waiting_mutex);

    pthread_mutex_lock(&s_mutex); 
 
}

/**leave serializer**/
//check if queue is  
//send signal to waiting queue

void leave_serializer(serial_t * s, queue_t * target_q)
{
	queues_t * qs = s->queues; 
    int sem_value = -314159;
    assert(qs->waiting_queue != NULL);

    int waitingQisEmpty = 1;
    int qId = 0;
    queue_t * notEmptyQ = NULL;
    if (qs->waiting_queue->count > 0){
            notEmptyQ = qs->waiting_queue;
            waitingQisEmpty = 0;
    }
    else {
        for (qId = 0; qId < s->nwqs; qId++)
            if (s->waitqs[qId] != NULL && s->waitqs[qId]->count > 0){
                waitingQisEmpty = 0;
                notEmptyQ = s->waitqs[qId];
            }
    }


    if (waitingQisEmpty == 0){
        sem_post(&(notEmptyQ->sem));
        sem_getvalue(&(notEmptyQ->sem), &sem_value);
        //printf("waiting queue: %x, sem value is %d, current Q size %d\n", notEmptyQ, sem_value, notEmptyQ->count);        
    }else if(qs->leave_crowd_queue->count > 0){
        sem_post(&(s->sem));
        sem_getvalue(&(s->sem), &sem_value);
        //printf("leave sem value is %d, current Q size %d\n", sem_value, qs->leave_crowd_queue->count);
    }else if(qs->enter_queue->count > 0){
        sem_post(&(s->sem));
        sem_getvalue(&(s->sem), &sem_value);
        //printf("enter sem value is %d, current Q size %d\n", sem_value, qs->enter_queue->count);         
    }else {
        assert(target_q != NULL);        
        
        
        if (target_q == qs->waiting_queue)
        {
            sem_post(&(target_q->sem));
            sem_getvalue(&(target_q->sem), &sem_value);
           // printf("targetq: qs->waiting_queue, value is %d, current Q size %d\n", sem_value, target_q->count);             
        }
        if (target_q == qs->leave_crowd_queue){
            sem_post(&(s->sem));
            sem_getvalue(&(s->sem), &sem_value);
           // printf("targetq: leave_crowd_queue, value is %d, current Q size %d\n", sem_value, target_q->count);             
        }
        if (target_q == qs->enter_queue){
            sem_post(&(s->sem));
            sem_getvalue(&(s->sem), &sem_value);
          //  printf("targetq: enter_queue, value is %d, current Q size %d\n", sem_value, target_q->count);             
        }
    }
}

/**leave_crowd_queue*/
void leave_crowd(serial_t * s, int priority){
	
    queues_t * qs = s->queues; 
    
    qs->leave_crowd_queue->enqueue(s, qs->leave_crowd_queue, 0);

    sem_wait(&(s->sem));



    qs->leave_crowd_queue->dequeue(qs->leave_crowd_queue);

    pthread_mutex_lock(&s_mutex);
}

void Serial_Join_Crowd(serial_t *s, crowd_t *c, void* (*func)())
{
    

	queues_t * qs = s->queues; 
    
    pthread_mutex_unlock(&s_mutex);

    pthread_mutex_lock(&(c->c_mutex));

/*    if (c == gs->rd_crowd)
        assert(gs->wr_crowd->count == 0);
    else{
         assert(gs->rd_crowd->count == 0);
         assert(gs->wr_crowd->count == 0);
    }*/
    c->count++;
    sem_post(&(s->count_sem));
    pthread_mutex_unlock(&(c->c_mutex));
    
    leave_serializer(s, qs->leave_crowd_queue);
    func();

    pthread_mutex_lock(&(c->c_mutex));
    c->count--;
    pthread_mutex_unlock(&(c->c_mutex));

    pthread_mutex_lock(&s_mutex);
}
