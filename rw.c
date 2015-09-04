/*
 * Reader and Writer's Problem for disk scheduler
 * CSE 511 Test Program
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "rw_serial.h"

#define NUM_READERS 5
#define NUM_WRITERS 5

int main(int argc, char *argv[])
{
	// Initialize vars
	int i;
	int rc;
    pthread_t reader[NUM_READERS];
	pthread_t writer[NUM_WRITERS];

	// Create serializer
	create();

	// Create the r/w requests (as threads) to the disk
	for (i = 0; i < NUM_READERS; i++)
	{
      rc = pthread_create(&reader[i], NULL, read_func, (void *)i);
      if (rc){
         printf("ERROR; return code from pthread_create() is %d\n", rc);
         exit(-1);
      }
	}
	for (i = 0; i < NUM_WRITERS; i++)
	{
      rc = pthread_create(&writer[i], NULL, write_func, (void *)i);
      if (rc){
         printf("ERROR; return code from pthread_create() is %d\n", rc);
         exit(-1);
      }
	}
	printf("Readers/Writers were created successfully!\n");

	// Wait until all threads are done
	for (i = 0; i < NUM_READERS; i++)
		pthread_join (reader[i], NULL);
	for (i = 0; i < NUM_WRITERS; i++)
		pthread_join (writer[i], NULL);
	printf("Reads/writes were done successfully!\n");

	// Finish
	return 0;
}
