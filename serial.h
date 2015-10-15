 /*******************************************************************************
 * File:     serial.h
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

//#ifndef __SERIAL_HH__
//#define __SERIAL_HH__
#include <semaphore.h>

pthread_mutex_t s_mutex; // what is this mutex for? I suppose it is for serializer(chen)
//pthread_mutex_t q_mutex;//enter Q mutex
pthread_mutex_t waiting_mutex;
struct serial_t ;



/*enum  kSERIALIZERSTATUS
{
    kNOPROCESS = -1,
    kPARENTPID = 0,
    kOTHERPID  = 1
};
*/
enum  kHASITEM
{
    kEMPTY = 1,
    kNOTEMPTY = 0
};

enum kNUM_WAITING_Q{
    kWAITINGQ0 = 0,
    kWAITINGQ1 = 1,
    kWAITINGQ2 = 2,
    kWAITINGQN = 99
};

enum  kDIRECTION
{
    kUP = 0,
    kDOWN = 1
};

struct crowd_t {
    int count;
    pthread_mutex_t c_mutex;
    pthread_mutex_t s_mutex;    
    int *fork;
    //pthread_mutex_t cylinder_mutex;
    //int cylinder;

    int total_count;
};

struct qnode {
    pid_t  tid;
    sem_t  sem; /**if I am the head, sem is 1*/
    int    priority; // large number with higher priority
    struct qnode *next;
    int current_cylinder;
};

struct queue_t {
    sem_t         sem;
    struct serial_t * s;
    struct qnode *_head;
    struct qnode *_tail;
    struct queue_t * (*enqueue)(struct serial_t * s, struct queue_t * q, int priority);
    struct qnode * (*dequeue)(struct queue_t * q);
    struct qnode * (*head)(struct queue_t * q);
    int count;
    pthread_mutex_t q_mutex;
    int isHeadChanged;
};

struct queues_t {
    struct queue_t *enter_queue;
    struct queue_t *leave_crowd_queue;
    struct queue_t *waiting_queue; 
    // waiting_queue is a pointer, 
    // to the queue of the second parameter Serial_Enqueue
    struct queue_t **waiting_queues;
};

/**
 * Here, we create three sets of queus.
 * enter_quene: 
 *                Threads waiting in this queue to gain serializer, 
 *              after gain the serializer, goto waiting Q.
 * waiting_queues: 
 *                Can be spilted into two or more queues, e.g. (Read Waiting Q,
 *              Write Waiting Queue, etc.). Threads waiting in this queues 
 *              is to gain serializer, after gain the serializer, goto 
 *              wthe resource, that is, crowd.
 * leave_crowd_queue:  
 *                After using the crowd, go to this queue, waiting for serializer
 *                , and call Exit().                  
 */
struct serial_t {
    struct queues_t * queues;
    struct crowd_t *rd_crowd;
    struct crowd_t *wr_crowd;
    sem_t  sem; // shared by enter_queue and leave_crowd_queue
    sem_t  count_sem;
    int direction;  // 0/kUP: up direction, from small to large
                    // 1/kDOWN: down direction, from large to small
    int cylinder;

    struct queue_t ** waitqs;
    int nwqs;
};

typedef  struct serial_t serial_t;
typedef  struct queues_t queues_t;
typedef  struct queue_t  queue_t;
typedef  struct crowd_t  crowd_t;
typedef  struct qnode    qnode_t;

typedef  int            cond_t;

serial_t * Create_Serial(); //done(haibo, chen)
void Serial_Enter(serial_t *); //done(haibo, chen)
void Serial_Exit(serial_t *);
queue_t * Create_Queue(serial_t *);
crowd_t * Create_Crowd(serial_t *);
int  Queue_Empty(serial_t *, queue_t *);
int  Crowd_Empty(serial_t *, crowd_t *);
void Serial_Enqueue(serial_t *, queue_t *, cond_t (*func)(), int priority);
void Serial_Join_Crowd(serial_t *, crowd_t *, void *(*func)());

queue_t * queue_t_enqueue(serial_t * s, queue_t *q, int priority);
qnode_t * queue_t_dequeue(queue_t * q);
qnode_t * queue_t_head(queue_t * q);
queues_t* create_queues(serial_t * serializer, const int waiting_queue_number);
queue_t* create_queue(serial_t * serializer, int initial_sem);
void leave_serializer(serial_t * s, queue_t * target_q);
void leave_crowd(serial_t * s, int priority);


serial_t* gs;
//#endif
 //__SERIAL_HH__
