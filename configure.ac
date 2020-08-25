
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.68])
AC_INIT([FPS], [1.0], [drbitboy@gmail.com])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
AC_CONFIG_SRCDIR([tyc2lib.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_LN_S
AC_PROG_MAKE_SET

# Checks for libraries.
AC_CHECK_LIB([sqlite3], [sqlite3_open])
AC_CHECK_LIB([curl], [curl_easy_init])
AC_CHECK_LIB([m], [sin])
#AC_CHECK_LIB([sqlite3], [sqlite3_open_v2])

# Checks for header files.
AC_PATH_X
AC_CHECK_HEADERS([malloc.h stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_SIZE_T
AC_HEADER_STDBOOL

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([bzero floor getcwd gettimeofday memmove memset mkdir pow putenv setenv sqrt strchr strdup strrchr strstr uname])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
