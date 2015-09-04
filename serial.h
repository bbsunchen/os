serial_t Create_Serial(); 
Serial_Enter(serial_t); 
Serial_Exit({serial_t); 
queue_t Create_Queue(serial_t); 
crowd_t Create_Crowd(serial_t); 
int Queue_Empty(serial_t, queue_t); 
int Crowd_Empty(serial_t, crowd_t); 
Serial_Enqueue(serial_t, queue_t, cond_t;(*func)()); 
Serial_Join_Crowd(serial_t,crowd_t,void*(*func)());