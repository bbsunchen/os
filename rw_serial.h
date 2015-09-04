#include "serial.h"

#define DELAY 100000

#define SIZE 10
long database[SIZE];

serial_t* serializer;
queue_t* waiting_q;
crowd_t* readers_crowd;
crowd_t* writers_crowd;

void create();

void *read_func(void *id);
void *write_func(void *id);
