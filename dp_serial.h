 /*******************************************************************************
 * File:     dp_serial.h
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

#include "serial.h"

serial_t* serializer;
queue_t* waiting_q;
crowd_t* eating_crowd;
crowd_t* thinking_crowd;

void Init_dp(int nphilosophers); 
void Eat(int phil_id, void *(*model_eat)()); 
void Think(int phil_id, void *(*model_think)());

