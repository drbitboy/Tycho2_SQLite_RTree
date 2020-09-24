#ifndef __GET_CLIENT_SOCKET_FD__
#define __GET_CLIENT_SOCKET_FD__

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#ifndef DEFAULT_PORT_STRING
#define DEFAULT_PORT_STRING "29073"
#endif

int get_client_socket_fd(char* argv1, char* service_prefix);

#endif // __GET_CLIENT_SOCKET_FD__
