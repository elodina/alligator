# Building and running Mesos with custom allocation module

1. Build Mesos with unbundled dependencies. From Mesos root folder execute:
./configure --with-glog=/usr --with-protobuf=/usr --with-boost=/usr
make
make install
(you may need to install third party packages prior to configurating)
2. Build Go allocation module. From the module root folder execute:
./bootstrap
mkdir build && cd build
../configure --with-mesos=/path/to/mesos/installation
make
make install
3. Run Mesos master from Mesos root folder with Go allocation module:
export LD_LIBRARY_PATH=/usr/local/lib
./bin/mesos-master.sh --ip=127.0.0.1 --work_dir=./mesos_master --allocator=org_apache_mesos_GoHierarchicalAllocator --modules='{"libraries":[{"file":"/usr/local/lib/mesos/libgoallocation.so", "modules":[{"name":"org_apache_mesos_GoHierarchicalAllocator"}]}]}'
4. Run Mesos slave from Mesos root folder:
export LD_LIBRARY_PATH=/usr/local/lib
./bin/mesos-slave.sh --master=127.0.0.1:5050
