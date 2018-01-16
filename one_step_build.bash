#!/bin/bash

touch -t $(date +%Y%m%d%H%M.%S) configure.ac Makefile.am aclocal.m4 Makefile.in configure
./configure
make
