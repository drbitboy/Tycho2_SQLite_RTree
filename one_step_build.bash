#!/bin/bash

touch --date="`date`" configure.ac Makefile.am aclocal.m4 Makefile.in configure
./configure
make
