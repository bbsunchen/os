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
queue_t* up_queue;
queue_t* down_queue;
crowd_t* disk_crowd;

sem_t 	cylinder_sem;

void Init_ds(int ncylinders); // ncylinders is total number of disk cylinders 
int Disk_Request(int cylinderno, void* model_request(), int *seekedcylinders, int tid); 
// cylinderno is requested cylinder. 
// 3rd parameter should return number of cylinders seeked from previous request to service this request. 
// Return value is the sequence number of service order of requests.
