# Building and running Mesos with custom allocation module

1. Build Mesos with unbundled dependencies. From Mesos root folder execute:
```
./configure --with-glog=/usr --with-protobuf=/usr --with-boost=/usr
make
make install
```
(you may need to install third party packages prior to configurating)
2. Build Kafka allocation module. From the module root folder execute:
```./bootstrap
mkdir build && cd build
../configure --with-mesos=/path/to/mesos/installation
make
make install
```
3. Run Zookeeper from its installation folder:
    bin/zkServer.sh start
4. Run Kafka from its installation folder:
    ./bin/kafka-server-start.sh config/server.properties
5. Run Mesos master from Mesos root folder with Kafka allocation module:
```
export LD_LIBRARY_PATH=/usr/local/lib

./bin/mesos-master.sh --ip=127.0.0.1 --work_dir=./mesos_master --allocator=org_apache_mesos_KafkaHierarchicalAllocator --modules='{"libraries":[{"file":"/usr/local/lib/mesos/libkafkaallocation.so", "modules":[{"name":"org_apache_mesos_KafkaHierarchicalAllocator"}]}]}'

6. Run Mesos slave from Mesos root folder:
```
export LD_LIBRARY_PATH=/usr/local/lib
./bin/mesos-slave.sh --master=127.0.0.1:5050
```
