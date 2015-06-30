/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "allocator_server.hpp"
#include <gflags/gflags.h>
#include <folly/Memory.h>
#include <folly/Portability.h>
#include <folly/io/async/EventBaseManager.h>
#include <httpserver/ResponseBuilder.h>
#include <unistd.h>
#include "./allocator.pb.h"

using namespace allocator;

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

using Protocol = HTTPServer::Protocol;

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class AllocatorHandlerFactory : public RequestHandlerFactory {
public:
  explicit AllocatorHandlerFactory(HierarchicalDRFAllocator* alloc)
  : allocator(alloc)
  {

  }

  void onServerStart() noexcept override
    {

    }

  void onServerStop() noexcept override
    {

    }

  RequestHandler* onRequest(RequestHandler*, HTTPMessage*) noexcept override
    {
    return new AllocatorHandler(allocator);
    }

private:
  HierarchicalDRFAllocator* allocator;
};

AllocatorServer::AllocatorServer()
:allocator(nullptr)
{

}

AllocatorServer::~AllocatorServer()
{

}

void AllocatorServer::initOptions(HTTPServerOptions& options)
{
  options.idleTimeout = std::chrono::milliseconds(60000);
  options.shutdownOn = {SIGINT, SIGTERM};
  options.enableContentCompression = true;
  std::unique_ptr<RequestHandlerFactory> factory(new AllocatorHandlerFactory(allocator));
  options.handlerFactories = RequestHandlerChain()
              .addThen(factory)
              .build();
}

bool AllocatorServer::onStarting()
{
  Try<mesos::master::allocator::Allocator*> try_allocator = HierarchicalDRFAllocator::create();
  //TODO check ownership
  if (try_allocator.isError())
    return false;
  allocator = dynamic_cast<HierarchicalDRFAllocator*>(try_allocator.get());
  return true;
}

HierarchicalDRFAllocator* AllocatorServer::getAllocator()
{
  return allocator;
}

AllocatorHandler::AllocatorHandler(HierarchicalDRFAllocator* alloc)
: allocator(alloc)
{

}

AllocatorHandler::~AllocatorHandler()
{

}

void AllocatorHandler::process(const std::string& type, const std::string& value)
{
  if (type == "AddSlave")
    addSlave(value);
  if (type == "AddFramework")
    addFramework(value);
}

void AllocatorHandler::addSlave(const std::string& data)
{
  std::cerr << "Parsing AddSlave\n";
  AddSlave proto;

  proto.ParseFromString(data);
  std::cerr << "Parsed slaveId value is " << proto.slaveid().value().c_str() <<
    "\nParsed slaveInfo hostname is " << proto.slaveinfo().hostname().c_str() << '\n';

  SlaveID slaveId = proto.slaveid();
  SlaveInfo slaveInfo = proto.slaveinfo();

  Resources total;
  for (int i = 0; i != proto.total_size(); ++i)
    total+=proto.total(i);

  hashmap<FrameworkID, Resources> used;
  for (int i = 0; i != proto.frameworkresources_size(); ++i)
  {
    Resources total;
    for (int j = 0; j != proto.frameworkresources(i).resources_size(); ++j)
      total+=proto.frameworkresources(i).resources(j);
    used.put(proto.frameworkresources(i).frameworkid(), total);
  }
  allocator->addSlave(slaveId, slaveInfo, total, used);
}

void AllocatorHandler::addFramework(const std::string& data)
{
  std::cerr << "Parsing AddFramework\n";
  AddFramework proto;

  proto.ParseFromString(data);
  std::cerr << "Parsed frameworkId value is " << proto.frameworkid().value().c_str() <<
    "\nParsed frameworkInfo name is " << proto.frameworkinfo().name().c_str() << '\n';

  FrameworkID frmId = proto.frameworkid();
  FrameworkInfo frmInfo = proto.frameworkinfo();

  hashmap<SlaveID, Resources> used;
  for (int i = 0; i != proto.slaveresources_size(); ++i)
  {
    Resources total;
    for (int j = 0; j != proto.slaveresources(i).resources_size(); ++j)
      total+=proto.slaveresources(i).resources(j);
    used.put(proto.slaveresources(i).slaveid(), total);
  }
  allocator->addFramework(frmId, frmInfo, used);
}

}}}}
