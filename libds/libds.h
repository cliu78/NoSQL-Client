/** @file libdslocal.h */
/* 
 * CS 241
 * The University of Illinois
 */

#ifndef _LIBDS_H_
#define _LIBDS_H_

#define LIBDS_NOSQL

#include <pthread.h>
typedef struct _datastore_t
{
	void *root;
	int client_fd;
	unsigned long rev;
} datastore_t;


void datastore_set_server(const char *server, int port);

void datastore_init(datastore_t *ds);

unsigned long datastore_put(datastore_t *ds, const char *key, const char *value);
const char *datastore_get(datastore_t *ds, const char *key, unsigned long *revision);
unsigned long datastore_update(datastore_t *ds, const char *key, const char *value, unsigned long known_revision);
unsigned long datastore_delete(datastore_t *ds, const char *key, unsigned long known_revision);

void datastore_destroy(datastore_t *ds);

#endif
