/** @file libmapreduce.c */
/* 
 * CS 241
 * The University of Illinois
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/select.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>
#include <poll.h>

#include "libmapreduce.h"
#include "libds/libds.h"


static const int BUFFER_SIZE = 2048;  /**< Size of the buffer used by read_from_fd(). */
//mapreduce_t mymapreduces;
pthread_t tid;
pid_t * process;
int sizeof_array(char ** result)
{
	return sizeof(result)/sizeof(result[0]);
}


/**
 * Adds the key-value pair to the mapreduce data structure.  This may
 * require a reduce() operation.
 *
 * @param key
 *    The key of the key-value pair.  The key has been malloc()'d by
 *    read_from_fd() and must be free()'d by you at some point.
 * @param value
 *    The value of the key-value pair.  The value has been malloc()'d
 *    by read_from_fd() and must be free()'d by you at some point.
 * @param mr
 *    The pass-through mapreduce data structure (from read_from_fd()).
 */
static void process_key_value(const char *key, const char *value, mapreduce_t *mr)
{
	const char * current_value = datastore_get( &(mr->datastore), key, &(mr->version) );
	if (current_value !=NULL )
	{
		const char * reduce_value = mr->reduce( current_value, value );

		datastore_update( &(mr->datastore), key, reduce_value, (mr->version) );
		free((char *)reduce_value);
		free((char *)current_value); 
	}
	else
	{
		datastore_put( &(mr->datastore), key, value );

	}

	free((char *)key);
	free((char *)value); 
}


/**
 * Helper function.  Reads up to BUFFER_SIZE from a file descriptor into a
 * buffer and calls process_key_value() when for each and every key-value
 * pair that is read from the file descriptor.
 *
 * Each key-value must be in a "Key: Value" format, identical to MP1, and
 * each pair must be terminated by a newline ('\n').
 *
 * Each unique file descriptor must have a unique buffer and the buffer
 * must be of size (BUFFER_SIZE + 1).  Therefore, if you have two
 * unique file descriptors, you must have two buffers that each have
 * been malloc()'d to size (BUFFER_SIZE + 1).
 *
 * Note that read_from_fd() makes a read() call and will block if the
 * fd does not have data ready to be read.  This function is complete
 * and does not need to be modified as part of this MP.
 *
 * @param fd
 *    File descriptor to read from.
 * @param buffer
 *    A unique buffer associated with the fd.  This buffer may have
 *    a partial key-value pair between calls to read_from_fd() and
 *    must not be modified outside the context of read_from_fd().
 * @param mr
 *    Pass-through mapreduce_t structure (to process_key_value()).
 *
 * @retval 1
 *    Data was available and was read successfully.
 * @retval 0
 *    The file descriptor fd has been closed, no more data to read.
 * @retval -1
 *    The call to read() produced an error.
 */
static int read_from_fd(int fd, char *buffer, mapreduce_t *mr)
{
	/* Find the end of the string. */
	int offset = strlen(buffer);

	/* Read bytes from the underlying stream. */
	int bytes_read = read(fd, buffer + offset, BUFFER_SIZE - offset);
	if (bytes_read == 0)
		return 0;
	else if(bytes_read < 0)
	{
		fprintf(stderr, "error in read.\n");
		return -1;
	}

	buffer[offset + bytes_read] = '\0';

	/* Loop through each "key: value\n" line from the fd. */
	char *line;
	while ((line = strstr(buffer, "\n")) != NULL)
	{
		*line = '\0';

		/* Find the key/value split. */
		char *split = strstr(buffer, ": ");
		if (split == NULL)
			continue;

		/* Allocate and assign memory */
		char *key = malloc((split - buffer + 1) * sizeof(char));
		char *value = malloc((strlen(split) - 2 + 1) * sizeof(char));

		strncpy(key, buffer, split - buffer);
		key[split - buffer] = '\0';

		strcpy(value, split + 2);

		/* Process the key/value. */
		process_key_value(key, value, mr);

		/* Shift the contents of the buffer to remove the space used by the processed line. */
		memmove(buffer, line + 1, BUFFER_SIZE - ((line + 1) - buffer));
		buffer[BUFFER_SIZE - ((line + 1) - buffer)] = '\0';
	}

	return 1;
}


/**
 * Initialize the mapreduce data structure, given a map and a reduce
 * function pointer.
 */
void mapreduce_init(mapreduce_t *mr, 
                    void (*mymap)(int, const char *), 
                    const char *(*myreduce)(const char *, const char *))
{	

	mr->map = mymap;
	mr->reduce = myreduce;

	
	datastore_init( &(mr->datastore) );

	mr->size = 0;
	mr->version = 0;
}
/**
 * Starts the map() processes for each value in the values array.
 * helper function
 */
void * work(void * temp)
{
	mapreduce_t * mr_working = (mapreduce_t *)temp;
	int  i= 0;
	
	for (i = 0; i < mr_working->size; ++i)
	{
		close(mr_working->pipelist[i][1]);
	}
	int set_size = mr_working->size;
	

	//fd_set master_read_fdset;
	fd_set active_read_fdset;
	FD_ZERO( &(mr_working->master_read_fdset) );
	FD_ZERO( &active_read_fdset);

	
	for (i = 0; i < mr_working->size; ++i)
	{
		FD_SET( mr_working->pipelist[i][0], &(mr_working->master_read_fdset) );
	}
	
	
	while(set_size > 0)
	{

		/* Copy the master set into the active set. */
        active_read_fdset = mr_working->master_read_fdset;
        /* Call select() */
        assert(-1 != select(FD_SETSIZE, &active_read_fdset, NULL, NULL, NULL) );

        /* Check if any of our FDs are in the result set */
        for (i = 0; i < (mr_working->size); ++i)
        {
        	if (FD_ISSET(mr_working->pipelist[i][0], &active_read_fdset)) 
        	{
	            const int numbytesread = read_from_fd(mr_working->pipelist[i][0], mr_working->buffer[i], mr_working);

	            if (numbytesread == 0) {
	                /* pipe has been closed -> close our read-end and
	                 * remove from master set
	                 */
	                close(mr_working->pipelist[i][0]);
	                FD_CLR(mr_working->pipelist[i][0], &(mr_working->master_read_fdset) );
	                set_size--;
	            }
	            else if (numbytesread == 1)
	            {
	                
	                //printf("got from child[%d]: [%s]\n", i, (mr_working->buffer[i]) );   
	         
	            }
	            else if (numbytesread == -1)
	            {
	            	printf("error when read from fd\n");
	            	exit(0);
	            }

	        }
        }
        
	}

	

	return NULL;
}

/**
 * Starts the map() processes for each value in the values array.
 * (See the MP description for full details.)
 */
void mapreduce_map_all(mapreduce_t *mr, const char **values)
{

	int size=0;
	while(values[size]!=0)
	{
		size++;
	}
	mr->size = size;

	mr->pipelist = (int ** )malloc(sizeof(int *)* (mr->size) );
	mr->buffer = (char **)malloc(sizeof(char*)*(mr->size));

	int i = 0;
	for (i = 0; i < (mr->size); ++i)
	{
		mr->pipelist[i] =  (int *)malloc(sizeof(int)*2);
		
		pipe(mr->pipelist[i]);
	}

	//mr->pipelist = pipelist;
	process = (pid_t *)malloc(sizeof(pid_t)*size); 
	
	for (i = 0; i < size; ++i)
	{
		;
		if (0 == fork() ) {
	        
	        close(mr->pipelist[i][0]);
	        mr->map(mr->pipelist[i][1], values[i]);
	        exit(0);
	    }
	    else
	    {
	    	close(mr->pipelist[i][1]);
	    	FD_SET(mr->pipelist[i][0], &(mr->master_read_fdset) );
	    	mr->buffer[i] = (char *)malloc(sizeof(char)* ( BUFFER_SIZE+1) );
			strcpy(mr->buffer[i], "\0");
	    }
	    	
	}
	
	pthread_create(&tid,NULL,&work,(void*)mr);	

	
	 
}


/**
 * Blocks until all the reduce() operations have been completed.
 * (See the MP description for full details.)
 */
void mapreduce_reduce_all(mapreduce_t *mr)
{
	
	pthread_join(tid, NULL);
	//char * test = "a";
	//const char * test_value = datastore_get( &(mr->datastore), test, &(mr->version) );
	//printf("the value of a %s\n", test_value);
}


/**
 * Gets the current value for a key.
 * (See the MP description for full details.)
 */
const char *mapreduce_get_value(mapreduce_t *mr, const char *result_key)
{
	
	const char * current_value = datastore_get( &(mr->datastore), result_key, &(mr->version) );
	
	
	return  current_value;
}


/**
 * Destroys the mapreduce data structure.
 */
void mapreduce_destroy(mapreduce_t *mr)
{

	datastore_destroy( &(mr->datastore) );
	int i=0;
	for ( i = 0; i < mr->size; ++i)
	{
		free(mr->buffer[i]);
		free( mr->pipelist[i] );
	}
	free( mr->buffer );
	free( mr->pipelist );
	free( process );

}