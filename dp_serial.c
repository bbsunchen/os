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
#include <string.h>
#include <pthread.h> 
#include <assert.h> 
#include "dp_serial.h"

enum FORKSTATUS{
    kIDLE = 0,
    kBUSY  = 1
};

int np;

/*extern serial_t* gs;*/

void Init_dp(int nphilosophers)
{
    np = nphilosophers;
    serializer = Create_Serial();
    waiting_q = Create_Queue(serializer);
    eating_crowd = Create_Crowd(serializer);
    thinking_crowd = Create_Crowd(serializer);
    
    eating_crowd->fork = (int *) malloc (np * sizeof(int));
    memset(eating_crowd->fork, '\0', np *sizeof(int)); // fork is kIDLEing;    

/*    gs = serializer;
    gs->rd_crowd = eating_crowd;
    gs->wr_crowd = thinking_crowd; */ 
        return ; 
}

cond_t eat_cond(int tid, crowd_t* c)
{
    int left  = ((tid - 1) + np) % np;
    int right = ((tid) + np) % np; 
    pthread_mutex_lock(&(eating_crowd->s_mutex));
    if ((c->fork)[left] != kBUSY && (c->fork)[right] != kBUSY)
    {
        
        (c->fork)[left] = kBUSY;  (c->fork)[right] = kBUSY;
        pthread_mutex_unlock(&(eating_crowd->s_mutex));
        return 1;
    }
    else
    { 
        pthread_mutex_unlock(&(eating_crowd->s_mutex));
        return 0;
    }
}

cond_t think_cond(int tid, crowd_t* c)
{
    return 1;
}

void Eat(int phil_id, void *(*model_eat)())
{
    cond_t eat_wrapper()
    {
        return eat_cond(phil_id, eating_crowd);
    }

    void* meat_wrapper()
    {
        model_eat(phil_id);
        return NULL;        
    }

    Serial_Enter(serializer);
    Serial_Enqueue(serializer, waiting_q, &eat_wrapper, 0);
    Serial_Join_Crowd(serializer, eating_crowd, &meat_wrapper);
    int left  = ((phil_id - 1) + np) % np;
    int right = ((phil_id) + np) % np;     
    int nbusy = 0;
    int forkId = 0;    
    pthread_mutex_lock(&(eating_crowd->s_mutex));

    for (forkId = 0; forkId < np; forkId++)
        if ((eating_crowd->fork)[forkId] == kBUSY)
            nbusy++;
    assert(nbusy <= (np - 1));
    (eating_crowd->fork)[left] = kIDLE;  (eating_crowd->fork)[right] = kIDLE;
    pthread_mutex_unlock(&(eating_crowd->s_mutex));
    Serial_Exit(serializer);
    
    return ;
}

void Think(int phil_id, void *(*model_think)())
{
    cond_t think_wrapper()
    {
        return think_cond(phil_id, thinking_crowd);;
    }  
    void* mthink_wrapper()
    {
        model_think(phil_id);
        return NULL;
    }  
    Serial_Enter(serializer);
    Serial_Enqueue(serializer, waiting_q, &think_wrapper, 0);
    Serial_Join_Crowd(serializer, thinking_crowd, &mthink_wrapper);
    Serial_Exit(serializer);
    
    return ;
}