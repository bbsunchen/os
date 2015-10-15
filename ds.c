/*
 * ds.c
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

//#include "ds.h"
#include "ds_serial.h"

#define DISK_SIZE 100
#define CylinderSeekTime 0.05

pthread_t*req_thread[DISK_SIZE * 10];
int CylinderNo		[DISK_SIZE * 10];
float arrival_time	[DISK_SIZE * 10];
int ServicedSeqNo	[DISK_SIZE * 10];
int SeekedCylinders	[DISK_SIZE * 10];
char input_fname[20];
char line[256];
long size;
int rc;

float servingTime[DISK_SIZE * 10];

// Call this function inside your crowd [DON'T CHANGE]
void model_request(int id, int seekedcylinders)
{
	printf("Request #%d is serving, seekedcyliners is %d.\n", id, seekedcylinders);
	sleep(seekedcylinders * CylinderSeekTime);
	servingTime[id] = seekedcylinders * CylinderSeekTime;
};

void *action(void* id)
{
	int tid = (int)id;
	ServicedSeqNo[tid] = -1;
	SeekedCylinders[tid] = -1;
	// Get the cylinder number, and model_request function
	// and return the serviced sequence number and the number of cylinders passed since last request
	ServicedSeqNo[tid] = Disk_Request(CylinderNo[tid], (void*)model_request, SeekedCylinders, tid);
	printf("Request #%d has been serviced.\n", tid);
	return NULL;
}

int main(int argc, char *argv[])
{
	argc = 2;
	argv[1] = "ds_inp1";
	// the command line arguments include an input filename
	if (argc != 2)
	{
	  printf("gcc -c -Wall -lpthread -o ds_test serial.c disk_scheduler.c ds_test.c\n");
	  printf("ds_test <testfile>\n");
	  exit(0);
	}

	// Initialize the disk
	Init_ds(DISK_SIZE);

	// Read the test file and get all requests
	// Initialize the requests
	strcpy(input_fname, argv[1]);
	FILE* file = fopen(input_fname, "r");
	size = 1;
	arrival_time[0] = 0;
	while (fgets(line, sizeof(line), file)) { // get line of the test file
		sscanf(line, "%f %d", arrival_time+size, CylinderNo+size); // get the arrival time and cylinder number
        //cylinder[size] = cylinder[size] % DISK_SIZE; // adjust the cylinder number to the disk size
		sleep(arrival_time[size] - arrival_time[size-1]);
		req_thread[size] = malloc(sizeof(pthread_t));
		rc = pthread_create(req_thread[size], NULL, action, (void*)size);
		if (rc)
		{
			printf("ERROR; return code from pthread_create() is %d\n", rc);
			exit(-1);
		}
		printf("Request #%d submitted @ %f.\n", size, arrival_time[size]);
		size++;
	}
	fclose(file);
	printf("Disk requests were created successfully!\n");

	// Verification
	// Wait until all threads are done
	int i;
	for (i = 1; i < size; i++)
		pthread_join ((*req_thread[i]), NULL);
	for(i = 1; i < size; i++)
	printf("RequestID = %d, ArrivalTime = %f, CylinderNo = %d, ServicedSeqNo = %d, SeekedCylinders = %d, servingTime = %f.\n",
			i, arrival_time[i], CylinderNo[i], ServicedSeqNo[i], SeekedCylinders[i], servingTime[i]);
	printf("The results printed successfully!\n");
	return 0;
}
