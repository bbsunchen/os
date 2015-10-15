 /******************************************************************************
 * File:     dp_serial.c
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
#include <string.h>
#include <pthread.h> 
#include <assert.h> 
#include "ds_serial.h"

cond_t disk_condition(){
    return Crowd_Empty(serializer, disk_crowd);
}


void Init_ds(int ncylinders)
{

    serializer = Create_Serial();
    up_queue = Create_Queue(serializer);
    down_queue = Create_Queue(serializer);
    disk_crowd = Create_Crowd(serializer);
    gs = serializer;
}

int Disk_Request(int cylinderno, void* model_request(), int *seekedcylinders, int tid){
    int serviceNo = 0; 
    void request_wrapper(){
        model_request(tid, seekedcylinders[tid]);
    }
    // what is tid used for? 
    Serial_Enter(serializer);// dequeue from the enter queue
    int current_direction = serializer->direction;
    int current_cylinder = serializer->cylinder;

    int priority = cylinderno;
    queue_t * waiting_q = NULL;
    if ( kUP == current_direction){
        priority *= -1; // In priority queue, large priority number has high priority
                        // in up direction, small priority has high priority.
        if (cylinderno < current_cylinder){
            waiting_q = down_queue;
        }else{
            waiting_q = up_queue;
        }
    }else if(kDOWN == current_direction){
        if (cylinderno > current_cylinder){
            waiting_q = up_queue;
        }else{
            waiting_q = down_queue;
        }
    }
    assert(waiting_q != NULL);
    
    

    Serial_Enqueue(serializer, waiting_q, disk_condition, priority);
    current_cylinder = serializer->cylinder;
    seekedcylinders[tid] = cylinderno - current_cylinder;
    if (seekedcylinders[tid] < 0){
        seekedcylinders[tid] *= -1; // return a positive number
    }
    serializer->cylinder = cylinderno;
    serviceNo = ++disk_crowd->total_count;
    Serial_Join_Crowd(serializer, disk_crowd, request_wrapper);

    if (serializer->queues->waiting_queue->count <= 0){
        // change direction
        if(serializer->queues->waiting_queue == up_queue){
            serializer->queues->waiting_queue = down_queue;
            serializer->direction = kDOWN;
        }else if (serializer->queues->waiting_queue == down_queue){
            serializer->queues->waiting_queue = up_queue;
            serializer->direction = kUP;
        }
    }


    Serial_Exit(serializer);

    return serviceNo;
}