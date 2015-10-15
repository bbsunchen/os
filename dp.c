/*
 * Reader and Writer's Problem for disk scheduler
 * CSE 511 Test Program
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include "dp_serial.h"

#define DELAY 100000
#define EatTime 1.0
#define ThinkTime 1.0
#define NUM_PHILOSOPHERS 5



void *model_eat(int id)
{
	printf("Philosopher #%d is eating.\n", id);
	double eat_time = ( rand() % (NUM_PHILOSOPHERS * 2) ) * EatTime - (NUM_PHILOSOPHERS - 1) * ThinkTime;
	sleep( (eat_time > 0) ? eat_time : EatTime);
	return NULL;
}

void *model_think(int id)
{
	printf("Philosopher #%d is thinking.\n", id);
	double think_time = ( rand() % (NUM_PHILOSOPHERS * 2) ) * ThinkTime - (NUM_PHILOSOPHERS - 1) * EatTime;
	sleep( (think_time > 0) ? think_time : ThinkTime);
	return NULL;
}

void *action(void* id)
{
	int tid = (int)id;
	while(1)
	{
		printf("Philosopher #%d is going to eat.\n", tid);
		Eat(tid, model_eat);
		printf("Philosopher #%d 's eating is done.\n", tid);
		Think(tid, model_think);
	}
	return NULL;
}

int main(int argc, char *argv[])
{
	// Initialize vars
	long i;
	int rc;
	pthread_t philosopher[NUM_PHILOSOPHERS];

	// Initialize the system
	Init_dp(NUM_PHILOSOPHERS);

	// Create the r/w requests (as threads) to the disk
	for (i = 0; i < NUM_PHILOSOPHERS; i++)
	{
		rc = pthread_create(&philosopher[i], NULL, action, (void *)i);
		if (rc)
		{
			printf("ERROR; return code from pthread_create() is %d\n", rc);
			exit(-1);
		}
	}
	printf("Philosophers created successfully!\n");

	// Wait until all threads are done
	for (i = 0; i < NUM_PHILOSOPHERS; i++)
		pthread_join (philosopher[i], NULL);
	printf("No more food!\n");

	// Finish
	return 0;
}
