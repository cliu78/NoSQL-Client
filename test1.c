/* 
 * CS 241
 * The University of Illinois
 */

#define _GNU_SOURCE
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>

#include "libmapreduce.h"
#include "libds/libds.h"

void map(int fd, const char *data)
{
	unsigned int i, letters = 0;
	for (i = 0; i < strlen(data); i++)
		if (isalpha(data[i]))
			letters++;

	char s[100];
	sprintf(s, "letters: %d\n", letters);

	write(fd, s, strlen(s));
	close(fd);
}

const char *reduce(const char *value1, const char *value2)
{
	int i1 = atoi(value1);
	int i2 = atoi(value2);

	char *result;
	asprintf(&result, "%d", (i1 + i2));
	return result;
}




void print_usage(char **argv)
{
	printf("Usage: %s <server> <port number>\n", argv[0]);
	exit(1);
}

int main(int argc, char **argv)
{

#ifdef LIBDS_NOSQL
	if (argc != 3)
		print_usage(argv);

	int port = atoi(argv[2]);
	if (port <= 0 || port >= 65536)
		print_usage(argv);

	datastore_set_server(argv[1], port);
#endif

	mapreduce_t mr;
	mapreduce_init(&mr, map, reduce);

	char *values[2];
	values[0] = "Some text";
	values[1] = NULL;
	mapreduce_map_all(&mr, (const char **)values);
	mapreduce_reduce_all(&mr);

	const char *s = "letters";
	const char *s1 = mapreduce_get_value(&mr, s);
	if (s1 == NULL)
		printf("%s: (null)\n", s);
	else
		printf("%s: %s\n", s, s1);



	mapreduce_destroy(&mr);
	return 0;
}