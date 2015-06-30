# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = -I m4

BUILT_SOURCES = $(top_srcdir)/allocator.pb.h \
$(top_srcdir)/allocator.pb.cc

# We want to install module in mesos directory.
pkglibdir = $(libdir)/mesos

# Initialize variables here so we can use += operator everywhere else.
pkglib_LTLIBRARIES = libgoallocation.la

# Add compiler and linker flags for pthreads.
AM_CXXFLAGS = $(PTHREAD_CFLAGS)
AM_LIBS = $(PTHREAD_LIBS)

# Setup CPPFLAGS that are used for most source files.
AM_CPPFLAGS = $(MESOS_CPPFLAGS) -I$(top_srcdir)/curlpp/include -I$(top_srcdir)/proxygen/proxygen -Wall -Werror

$(top_srcdir)/allocator.pb.h $(top_srcdir)/allocator.pb.cc: $(top_srcdir)/allocator.proto
	/usr/bin/protoc -I$(top_srcdir)/ -I/home/dzhyrova/Development/mesos-master/include --cpp_out=$(top_srcdir)/ $(top_srcdir)/allocator.proto

libgoallocation_la_SOURCES = allocator.pb.h \
allocator.pb.cc \
client.hpp \
client.cpp \
server.hpp \
server.cpp \
allocator_server.hpp \
allocator_server.cpp \
hook_server.hpp \
hook_server.cpp \
go_allocator.cpp \
go_hook.cpp
 
libgoallocation_la_LDFLAGS = -pthread
libgoallocation_la_LIBADD = -lproxygenhttpserver -lfolly -lcurlpp -lboost_system -lboost_thread -lglog -lgflags




