/** @file libds.c */
/* 
 * CS 241
 * The University of Illinois
 */


#include "libds.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <search.h>
#include <stddef.h>

#include "jansson.h"
#include "libhttp.h"


#define MAXSIZE (1024)


typedef struct _datastore_entry_t
{
	const char *key, *value;
	unsigned long rev;
} datastore_entry_t;
char server_name[1024];
int port_number;

/********************************************************
			helper fucntion
********************************************************/
 /** Private. */


/**
 * Sets the server the data store should connect to for
 * NoSQL operations.  You may assume this call will be 
 * made before any calls to datastore_init().
 *
 * @param server
 *   The hostname of the server.
 * @param port
 *   The post on the server to connect to.
 */
void datastore_set_server(const char *server, int port)
{
		server_name[0] = '\0';
		strcpy(server_name,server); 
		port_number = port;
}


/**
 * Initializes the data store.
 *
 * @param ds
 *    An uninitialized data store.
 */
void datastore_init(datastore_t *ds)
{
	ds->root = NULL;
	//connect to server
	struct sockaddr_in my_addr;
	my_addr.sin_family = AF_INET;
	my_addr.sin_addr.s_addr = htonl(INADDR_ANY); //inet_addr(server_name);//
	my_addr.sin_port = htons(port_number);
	socklen_t client_len = sizeof(my_addr);
 
	ds->client_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (ds->client_fd < 0) {
		perror("socket");
		exit(1);
	}
	int connect_flag = connect(ds->client_fd, (struct sockaddr *) &my_addr, client_len);
	if (connect_flag < 0)
	{ 
	perror("connect");
	exit(1);
	}
	//printf("get connection with server%s\n", server_name);

}


/**
 * Adds the key-value pair (key, value) to the data store, if and only if the
 * key does not already exist in the data store.
 *
 * @param ds
 *   An initialized data store.
 * @param key
 *   The key to be added to the data store.
 * @param value
 *   The value to associated with the new key.
 *
 * @retval 0
 *   The key already exists in the data store.
 * @retval non-zero
 *   The revision number assigned to the specific value for the given key.
 */
unsigned long datastore_put(datastore_t *ds, const char *key, const char *value)
{
	json_t * putobject = json_object();
	int resput = json_object_set_new(putobject,"Value", json_string(value) );
	if (resput != 0)
	{
		perror("set json value");
		exit(1);
	}
	char request_header[MAXSIZE];
	char request_body[MAXSIZE];
	request_header[0] = '\0';
	request_body[0] = '\0';
	char * objstr = json_dumps(putobject,0);
	printf("put send:%s\n", objstr);
	int bodysize = sprintf(request_body, "%s", objstr );

	int scan_size = 0; 
	scan_size += sprintf(request_header, "PUT /%s HTTP/1.1\r\n", key);
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "Content-Type: application/json\r\n");
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "Content-Length: %d\r\n", bodysize );
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "\r\n");
	send(ds->client_fd, request_header, scan_size, 0);
	send(ds->client_fd, request_body, bodysize, 0);
	json_decref(putobject);
	free(objstr);
	http_t http_response;
	int res = http_read( &http_response, ds->client_fd );
		if(res <= 0)
					  { //free(http_response);
					    perror("http_read");//break;// nothing to receive
					    exit(1);
					  }
		size_t contentlength = atoi( http_get_header(&http_response, "Content-Length") );
		const char * bodyres = http_get_body(&http_response,  &contentlength );
		//printf("put rec body is :%s\n", (char *)bodyres );
		const char * statusres = http_get_status(&http_response);

		json_error_t error;  
 		json_t *body = json_loads( bodyres, 0, &error );
 	if (strstr(statusres, "201")== NULL ||json_object_get(body, "ok") == NULL || json_object_get(body, "error") != NULL)
 	{
			json_decref(body);
			http_free(&http_response);
			return 0;
	} 
	else
		{
			//free(body);
			json_t * _rev = json_object_get( body, "rev" );
			const char * str_rev = json_string_value( _rev );
			//printf("str_rev is:%s\n", str_rev);
			unsigned long revision = atoi(str_rev);
			json_decref(body);
			http_free(&http_response);
			//printf("return value is :%d\n", json_number_value(&revision) );
			return revision;
		}
	
		
}


/**
 * Retrieves the current value, and its revision number, for a specific key.
 *
 * @param ds
 *   An initialized data store.
 * @param key
 *   The specific key to retrieve the value.
 * @param revision
 *   If non-NULL, the revision number of the returned value will be written
 *   to the address pointed to by <code>revision</code>.
 *
 * @return
 *   If the data store contains the key, a new string containing the value
 *   will be returned.  It is the responsibility of the user of the data
 *   store to free the value returned.  If the data store does not contain
 *   the key, NULL will be returned and <code>revision</code> will be unmodified.
 */
const char *datastore_get(datastore_t *ds, const char *key, unsigned long *revision)
{
		char * request_header = (char *)malloc( sizeof(char)*( strlen(key) + 22 ) );
		request_header[0] = '\0';
		int scan_size = 0;
		scan_size += sprintf(request_header, "GET /%s HTTP/1.1\r\n", key);
		scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "\r\n");
		send(ds->client_fd, request_header, scan_size, 0);
		free(request_header);
		http_t http_request;
		int res = http_read( &http_request, ds->client_fd );
		if(res <= 0)
					  {
					    perror("http_read");//break;// nothing to receive
					    exit(1);
					  }
		const char * statusres = http_get_status(&http_request);

		size_t contentlength = atoi( http_get_header(&http_request, "Content-Length") );
		const char * bodyres = http_get_body(&http_request,  &contentlength );
		json_error_t error;  
 		json_t *body = json_loads( bodyres, 0, &error ); 
		if (strstr(statusres, "200")== NULL ||json_object_get(body, "_id") == NULL || json_object_get(body, "error") != NULL)
		{
			json_decref(body);
			http_free(&http_request);
			printf("it is a null\n");
			return NULL;
		}
		else
		{
			json_t * Value = json_object_get( body, "Value" );
			const char * ret = strdup(json_string_value(Value));
			printf("string value is:%s\n", json_string_value(Value));
			//char * ret = json_string_value(Value);
			if (revision != NULL)
				{
					json_t * _rev = json_object_get( body, "_rev" );
					*revision = atoi(json_string_value( _rev ));
					printf("revision number is:%lu\n", *revision);
				}

			
			
			
			json_decref(Value);
			json_decref(body);
			http_free(&http_request);
			printf("return value is:%s\n",ret );
			return ret;
			
		}

}


/**
 * Updates the specific key in the data store if and only if the
 * key exists in the data store and the key's revision in the data
 * store matches the knwon_revision specified.
 * 
 * @param ds
 *   An initialized data store.
 * @param key
 *   The specific key to update in the data store.
 * @param value
 *   The updated value to the key in the data store.
 * @param known_revision
 *   The revision number for the key specified that is expected to be found
 *   in the data store.  If the revision number specified in calling the
 *   function does not match the revision number in the data store, this
 *   function will not update the data store.
 *
 * @retval 0
 *    The revision number specified did not match the revision number in the
 *    data store or the key was not found in the data store.  If the key is
 *    in the data store, this indicates that the data has been modified since
 *    you last performed a datastore_get() operation.  You should get an
 *    updated value from the data store.
 * @retval non-zero
 *    The new revision number for the key, now associated with the new value.
 */
unsigned long datastore_update(datastore_t *ds, const char *key, const char *value, unsigned long known_revision)
{
	json_t * updateobject = json_object();
	char revisionstr[33];sprintf(revisionstr,"%lu",known_revision);
	int resput = json_object_set_new(updateobject,"Value", json_string(value) );
	int resput2 = json_object_set_new(updateobject,"_rev", json_string(revisionstr) );
	if (resput != 0 || resput2 != 0)
	{
		perror("set json value");
		exit(1);
	}
	char request_header[MAXSIZE];
	char request_body[MAXSIZE];
	request_header[0] = '\0';
	request_body[0] = '\0';
	//int bodysize = sprintf(request_body, "%s", json_dumps(updateobject,0) );
	char * objstr = json_dumps(updateobject,0);
	int bodysize = sprintf(request_body, "%s", objstr );
	printf("update send:%s\n", objstr);
	int scan_size = 0; 
	scan_size += sprintf(request_header, "PUT /%s HTTP/1.1\r\n", key);
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "Content-Type: application/json\r\n");
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "Content-Length: %d\r\n", bodysize );
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "\r\n");

	send(ds->client_fd, request_header, scan_size, 0);
	send(ds->client_fd, request_body, bodysize, 0);
	json_decref(updateobject);
	free(objstr);
	http_t http_response;
	int res = http_read( &http_response, ds->client_fd );
		if(res <= 0)
					  { //free(http_response);
					    perror("http_read");//break;// nothing to receive
					    exit(1);
					  }
		size_t contentlength = atoi( http_get_header(&http_response, "Content-Length") );
		//const json_t * body = (json_t*)http_get_body(&http_response,  &contentlength );
		const char * bodyres = http_get_body(&http_response,  &contentlength );
		printf("update rec body is :%s\n", bodyres );
		json_error_t error;  
	 	json_t *body = json_loads( bodyres, 0, &error ); 
	if(json_object_get(body, "error") == NULL || json_object_get(body, "ok") != NULL)
		{
			printf("recv body is ok\n");
			json_t * _rev = json_object_get( body, "rev" );
			const char * str_rev = json_string_value( _rev );
			//printf("str_rev (update)is:%s\n", str_rev);
			unsigned long revision = atoi(str_rev);
			//unsigned long revision = atoi(json_string_value( _rev ));
			json_decref(body);
			printf("revision number is:%lu!!!!\n", revision);
			http_free(&http_response);
			return revision;
		}
	else
		{
			printf("recv body is error\n");
			json_decref(body);
			http_free(&http_response);
			return 0;
		}
}


/**
 * Deletes a specific key from the data store.
 *
 * @param ds
 *   An initialized data store.
 * @param key
 *   The specific key to update in the data store.
 * @param known_revision
 *   The revision number for the key specified that is expected to be found
 *   in the data store.  If the revision number specified in calling the
 *   function does not match the revision number in the data store, this
 *   function will not update the data store.
 *
 * @retval 0
 *    The revision number specified did not match the revision number in the
 *    data store or the key was not found in the data store.  If the key is
 *    in the data store, this indicates that the data has been modified since
 *    you last performed a datastore_get() operation.  You should get an
 *    updated value from the data store.
 * @retval non-zero
 *    The key was deleted from the data store.
 */
unsigned long datastore_delete(datastore_t *ds, const char *key, unsigned long known_revision)
{
	char request_header[MAXSIZE];
	request_header[0] = '\0';

	int scan_size = 0; 
	scan_size += sprintf(request_header, "DELETE /%s HTTP/1.1\r\n", key);
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "If-Match: %lu\r\n", known_revision);
	scan_size += sprintf(request_header + (ptrdiff_t)scan_size, "\r\n");
	//printf("datastore_update send header is:%s\n",request_header);
	send(ds->client_fd, request_header, scan_size, 0);
	http_t http_response;
	int res = http_read( &http_response, ds->client_fd );
		if(res <= 0)
					  { //free(http_response);
					    perror("http_read");//break;// nothing to receive
					    exit(1);
					  }
		size_t contentlength = atoi( http_get_header(&http_response, "Content-Length") );
		const char * bodyres = http_get_body(&http_response,  &contentlength );
		//printf("update rec body is :%s\n", (char *)body );
		json_error_t error;  
 		json_t *body = json_loads( bodyres, 0, &error ); 
	if(json_object_get(body, "ok") != NULL || json_object_get(body, "ok") == (json_t *)"true")
		{
			//free(body);
			json_t rev= *(json_object_get(body,  "rev"));
			const char * str_rev = json_string_value( &rev );
			unsigned long ret = atoi(str_rev);
			//unsigned long ret =  atoi(json_string_value( &rev ));
			json_decref(body);
			http_free(&http_response);
			return ret;
		}
	else
		{
			json_decref(body);
			http_free(&http_response);
			return 0;
		}
}


/**
 * Destroys the data store, freeing any memory that is in use by the
 * data store.
 *
 * @param ds
 *   An initialized data store.
 */
void datastore_destroy(datastore_t *ds)
{
	//free(ds);
	close(ds->client_fd);
}



