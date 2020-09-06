#include "get_client_socket_fd.h"

/***********************************************************************
 * Connect to "server/port#" and return socket file descriptor (fd)
 */
int
get_client_socket_fd(char* argv1, char* service_prefix)
{
struct addrinfo hints;
struct addrinfo* result = (struct addrinfo*) NULL;
struct addrinfo* rp = (struct addrinfo*) NULL;
int sock_fd = -1024;
int status;

char s_port_string[256];
char s_colon[2];
char s_hostname[1024];
char s_slash[2];
char s_portno[6] = DEFAULT_PORT_STRING;
char s_nothing[2];
int i_sscanf;

int fail_return = 0;

  i_sscanf = sscanf(argv1
                   ,"%255[^:]%1[:]%1023[^/]%1[/]%5[0-9]%1s"
                   ,s_port_string
                   ,s_colon
                   ,s_hostname
                   ,s_slash
                   ,s_portno
                   ,s_nothing
                   );

  --fail_return;
  if ( 0!=strcmp(s_port_string,service_prefix)
    || (i_sscanf!=3 && i_sscanf!=5)
     ) {
    fprintf(stderr, "invalid %s:... specification, argv[1] = '%s'\n"
                  , service_prefix
                  , argv1
                  );
    return fail_return;
  }

  /* Obtain address(es) matching host/port */

  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET;       /* Allow IPv4, not IPv6 */
  hints.ai_socktype = SOCK_STREAM; /* Stream socket */
  hints.ai_flags = 0;
  hints.ai_protocol = 0;          /* Any protocol */

  status = getaddrinfo(s_hostname, s_portno, &hints, &result);
  --fail_return;
  if (status != 0) {
    fprintf(stderr, "getaddrinfo: %s; argv[1] = '%s')\n"
                  , gai_strerror(status)
                  , argv1
                  );
    return fail_return;
  }

  /* getaddrinfo() returns a list of address structures.
     Try each address until we successfully connect(2).
     If socket(2) (or connect(2)) fails, we (close the socket
     and) try the next address. */

  for (rp = result; rp != NULL; rp = rp->ai_next) {
    sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sock_fd == -1) {
      continue;
    }

    if (connect(sock_fd, rp->ai_addr, rp->ai_addrlen) != -1) {
      break;                      /* Address rp->ai_addr succeeded */
    }

    (void)close(sock_fd);
  }

  --fail_return;
  if (rp == NULL) {               /* No address succeeded */
    fprintf(stderr, "Could not connect; argv[1] = '%s'\n", argv1);
    return fail_return;
  }

  freeaddrinfo(result);           /* Free getaddrinfo(...) result */

  return sock_fd;
}

#ifdef TEST_MAIN
/***********************************************************************
 * ./tst gaia:<hostname>[/<port>]
 * gcc -DTEST_MAIN -g -O0 tst.c -o tst
 * 
 * Sample Usage
 */
int
main(int argc, char** argv)
{
int server_sock_fd;
  if (argc < 2) {
    fprintf(stderr
           ,"Usage: %s gaia:<host>[/<port>]; default port = 29073\n"
           ,argv[0]
           );
    return (EXIT_FAILURE);
  }
  server_sock_fd = get_client_socket_fd(argv[1],"gaia");
  return server_sock_fd < -1024 ? server_sock_fd : (EXIT_SUCCESS);
}
#endif
