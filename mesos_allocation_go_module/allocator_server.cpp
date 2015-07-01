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

const std::map<std::string, AllocatorHandler::EEventType> AllocatorHandler::event_type =
   {{"AddFramework", ET_AddFramework},
    {"RemoveFramework", ET_RemoveFramework},
    {"ActivateFramework", ET_ActivateFramework},
    {"DeactivateFramework", ET_DeactivateFramework},
    {"AddSlave", ET_AddSlave},
    {"RemoveSlave", ET_RemoveSlave},
    {"UpdateSlave", ET_UpdateSlave},
    {"ActivateSlave", ET_ActivateSlave},
    {"DeactivateSlave", ET_DeactivateSlave},
    {"UpdateWhitelist", ET_UpdateWhitelist},
    {"RequestResources", ET_RequestResources},
    {"UpdateAllocation", ET_UpdateAllocation},
    {"RecoverResources", ET_RecoverResources},
    {"ReviveOffers", ET_ReviveOffers},
    {"UpdateFramework", ET_UpdateFramework}};

AllocatorHandler::AllocatorHandler(HierarchicalDRFAllocator* alloc)
: allocator(alloc)
{

}

AllocatorHandler::~AllocatorHandler()
{

}

void AllocatorHandler::process(const std::string& type, const std::string& value)
{
  switch(event_type.find(type)->second)
  {
  case ET_AddFramework:
    addFramework(value);
    break;
  case ET_RemoveFramework:
    removeFramework(value);
    break;
  case ET_ActivateFramework:
    activateFramework(value);
    break;
  case ET_DeactivateFramework:
    deactivateFramework(value);
    break;
  case ET_AddSlave:
    addSlave(value);
    break;
  case ET_RemoveSlave:
    removeSlave(value);
    break;
  case ET_UpdateSlave:
    updateSlave(value);
    break;
  case ET_ActivateSlave:
    activateSlave(value);
    break;
  case ET_DeactivateSlave:
    deactivateSlave(value);
    break;
  case ET_UpdateWhitelist:
    updateWhitelist(value);
    break;
  case ET_RequestResources:
    requestResources(value);
    break;
  case ET_UpdateAllocation:
    updateAllocation(value);
    break;
  case ET_RecoverResources:
    recoverResources(value);
    break;
  case ET_ReviveOffers:
    reviveOffers(value);
    break;
  case ET_UpdateFramework:
    updateFramework(value);
    break;
  default:
    std::cerr << "Unknown event type: " << type << "\n";
  }
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

void AllocatorHandler::removeFramework(const std::string& data)
{
  std::cerr << "Parsing RemoveFramework\n";
  RemoveFramework proto;

  proto.ParseFromString(data);

  FrameworkID frmId = proto.frameworkid();
  allocator->removeFramework(frmId);
}

void AllocatorHandler::activateFramework(const std::string& data)
{
  std::cerr << "Parsing ActivateFramework\n";
  ActivateFramework proto;

  proto.ParseFromString(data);

  FrameworkID frmId = proto.frameworkid();
  allocator->activateFramework(frmId);
}

void AllocatorHandler::deactivateFramework(const std::string& data)
{
  std::cerr << "Parsing DeactivateFramework\n";
  DeactivateFramework proto;

  proto.ParseFromString(data);

  FrameworkID frmId = proto.frameworkid();
  allocator->deactivateFramework(frmId);
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

void AllocatorHandler::removeSlave(const std::string& data)
{
  std::cerr << "Parsing RemoveSlave\n";
  RemoveSlave proto;

  proto.ParseFromString(data);

  SlaveID slaveId = proto.slaveid();

  allocator->removeSlave(slaveId);
}

void AllocatorHandler::updateSlave(const std::string& data)
{
  std::cerr << "Parsing UpdateSlave\n";
  UpdateSlave proto;

  proto.ParseFromString(data);

  SlaveID slaveId = proto.slaveid();

  Resources total;
  for (int i = 0; i != proto.resources_size(); ++i)
    total+=proto.resources(i);

  allocator->updateSlave(slaveId, total);
}

void AllocatorHandler::activateSlave(const std::string& data)
{
  std::cerr << "Parsing ActivateSlave\n";
  ActivateSlave proto;

  proto.ParseFromString(data);

  SlaveID slaveId = proto.slaveid();

  allocator->activateSlave(slaveId);
}

void AllocatorHandler::deactivateSlave(const std::string& data)
{
  std::cerr << "Parsing DeactivateSlave\n";
  DeactivateSlave proto;

  proto.ParseFromString(data);

  SlaveID slaveId = proto.slaveid();

  allocator->deactivateSlave(slaveId);
}

void AllocatorHandler::updateWhitelist(const std::string& data)
{
  std::cerr << "Parsing UpdateWhitelist\n";
  UpdateWhitelist proto;

  proto.ParseFromString(data);

  hashset<std::string> whitelist;
  for (int i = 0; i != proto.whitelist_size(); ++i)
    whitelist.insert(proto.whitelist(i));

  allocator->updateWhitelist(whitelist);
}

void AllocatorHandler::requestResources(const std::string& data)
{
  std::cerr << "Parsing RequestResources\n";
  RequestResources proto;

  proto.ParseFromString(data);
  FrameworkID frmId = proto.frameworkid();
  std::vector<Request> requests;
  for (int i = 0; i != proto.requests_size(); ++i)
    requests.push_back(proto.requests(i));

  allocator->requestResources(frmId, requests);
}

void AllocatorHandler::updateAllocation(const std::string& data)
{
  std::cerr << "Parsing UpdateAllocation\n";
  UpdateAllocation proto;

  proto.ParseFromString(data);

  FrameworkID frmId = proto.frameworkid();
  SlaveID slaveId = proto.slaveid();

  std::vector<Offer::Operation> operations;
  for (int i = 0; i != proto.operations_size(); ++i)
    operations.push_back(proto.operations(i));

  allocator->updateAllocation(frmId, slaveId, operations);
}

void AllocatorHandler::recoverResources(const std::string& data)
{
  std::cerr << "Parsing RecoverResources\n";
  RecoverResources proto;

  proto.ParseFromString(data);

  FrameworkID frmId = proto.frameworkid();
  SlaveID slaveId = proto.slaveid();

  Resources resources;
  for (int i = 0; i != proto.resources_size(); ++i)
    resources += proto.resources(i);

  ::Option<Filters> filters;
  if (proto.has_filters())
    filters = proto.filters();

  allocator->recoverResources(frmId, slaveId, resources,  filters);
}

void AllocatorHandler::reviveOffers(const std::string& data)
{
  std::cerr << "Parsing ReviveOffers\n";
  ReviveOffers proto;

  proto.ParseFromString(data);

  FrameworkID frmId = proto.frameworkid();

  allocator->reviveOffers(frmId);
}

void AllocatorHandler::updateFramework(const std::string& data)
{
  std::cerr << "Parsing UpdateFramework\n";
  UpdateFramework proto;

  proto.ParseFromString(data);

  FrameworkID frmId = proto.frameworkid();
  FrameworkInfo frmInfo = proto.frameworkinfo();

  allocator->updateFramework(frmId, frmInfo);
}

}}}}
