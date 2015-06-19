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

#include <memory>
#include <mesos/mesos.hpp>
#include <mesos/master/allocator.hpp>
#include <mesos/module.hpp>
#include <mesos/module/allocator.hpp>
#include <master/allocator/mesos/hierarchical.hpp>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include "./client.hpp"
#include "./server.hpp"
#include "./allocator.pb.h"

using namespace allocator;

using namespace mesos::internal::master::allocator;

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class GoAllocator : public mesos::master::allocator::Allocator
{
private:
  Client client;
  Server server;

public:
  // Factory to allow for typed tests.
  static Try<mesos::master::allocator::Allocator*> create();

  ~GoAllocator();

  void initialize(
    const Duration& allocationInterval,
    const lambda::function<
    void(const FrameworkID&,
      const hashmap<SlaveID, Resources>&)>& offerCallback,
      const hashmap<std::string, mesos::master::RoleInfo>& roles);

  void addFramework(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const hashmap<SlaveID, Resources>& used);

  void removeFramework(
    const FrameworkID& frameworkId);

  void activateFramework(
    const FrameworkID& frameworkId);

  void deactivateFramework(
    const FrameworkID& frameworkId);

  void addSlave(
    const SlaveID& slaveId,
    const SlaveInfo& slaveInfo,
    const Resources& total,
    const hashmap<FrameworkID, Resources>& used);

  void removeSlave(
    const SlaveID& slaveId);

  void updateSlave(
    const SlaveID& slave,
    const Resources& oversubscribed);

  void activateSlave(
    const SlaveID& slaveId);

  void deactivateSlave(
    const SlaveID& slaveId);

  void updateWhitelist(
    const ::Option<hashset<std::string> >& whitelist);

  void requestResources(
    const FrameworkID& frameworkId,
    const std::vector<Request>& requests);

  void updateAllocation(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const std::vector<Offer::Operation>& operations);

  void recoverResources(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const Resources& resources,
    const ::Option<Filters>& filters);

  void reviveOffers(
    const FrameworkID& frameworkId);

  virtual void updateFramework(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo);

private:
  GoAllocator();
  GoAllocator(const GoAllocator&); // Not copyable.
  GoAllocator& operator=(const GoAllocator&); // Not assignable.
};

Try<mesos::master::allocator::Allocator*>
GoAllocator::create()
{
  std::cerr << "Created Go allocator\n ";
  mesos::master::allocator::Allocator* allocator =
    new GoAllocator();
  return CHECK_NOTNULL(allocator);
}

GoAllocator::GoAllocator()
{

}

GoAllocator::~GoAllocator()
{

}

void GoAllocator::initialize(
  const Duration& allocationInterval,
  const lambda::function<
  void(const FrameworkID&,
    const hashmap<SlaveID, Resources>&)>& offerCallback,
    const hashmap<std::string, mesos::master::RoleInfo>& roles)
{
  server.getAllocator()->initialize(allocationInterval, offerCallback, roles);
}

inline void GoAllocator::addFramework(
  const FrameworkID& frameworkId,
  const FrameworkInfo& frameworkInfo,
  const hashmap<SlaveID, Resources>& used)
{

}

void GoAllocator::updateFramework(
  const FrameworkID& frameworkId,
  const FrameworkInfo& frameworkInfo)
{

}

inline void GoAllocator::removeFramework(
  const FrameworkID& frameworkId)
{

}

inline void GoAllocator::activateFramework(
  const FrameworkID& frameworkId)
{

}

inline void GoAllocator::deactivateFramework(
  const FrameworkID& frameworkId)
{

}

inline void GoAllocator::addSlave(
  const SlaveID& slaveId,
  const SlaveInfo& slaveInfo,
  const Resources& total,
  const hashmap<FrameworkID, Resources>& used)
{
  SlaveID* copy_slaveId = new SlaveID(slaveId);
  SlaveInfo* copy_slaveInfo = new SlaveInfo(slaveInfo);
  AddSlave proto;
  proto.set_allocated_slaveid(copy_slaveId);
  proto.set_allocated_slaveinfo(copy_slaveInfo);

  for (Resources::const_iterator it = total.begin(); it != total.end(); ++it)
  {
    Resource* res = proto.add_total();
    *res = *it;
  }

  for (typename hashmap<FrameworkID, Resources>::const_iterator it = used.cbegin(); it != used.cend(); ++it)
  {
    FrameworkResources* frm_res= proto.add_frameworkresources();
    FrameworkID* copy_framework_id = new FrameworkID(it->first);
    frm_res->set_allocated_frameworkid(copy_framework_id);
    for (Resources::const_iterator rit = it->second.begin(); rit != it->second.end(); ++rit)
    {
      Resource* res = frm_res->add_resources();
      *res = *rit;
    }
  }

  client.postData("AddSlave", proto);
}

inline void GoAllocator::removeSlave(
  const SlaveID& slaveId)
{

}

inline void GoAllocator::updateSlave(
  const SlaveID& slaveId,
  const Resources& oversubscribed)
{

}

inline void GoAllocator::activateSlave(
  const SlaveID& slaveId)
{

}

inline void GoAllocator::deactivateSlave(
  const SlaveID& slaveId)
{

}

inline void GoAllocator::updateWhitelist(
  const ::Option<hashset<std::string> >& whitelist)
{

}

inline void GoAllocator::requestResources(
  const FrameworkID& frameworkId,
  const std::vector<Request>& requests)
{

}

inline void GoAllocator::updateAllocation(
  const FrameworkID& frameworkId,
  const SlaveID& slaveId,
  const std::vector<Offer::Operation>& operations)
{

}

inline void GoAllocator::recoverResources(
  const FrameworkID& frameworkId,
  const SlaveID& slaveId,
  const Resources& resources,
  const ::Option<Filters>& filters)
{

}

inline void GoAllocator::reviveOffers(
  const FrameworkID& frameworkId)
{

}

}
}
}
}

static mesos::master::allocator::Allocator* createGoAllocator(const mesos::Parameters& parameters)
{
  return mesos::master::allocator::custom::GoAllocator::create().get();
}

mesos::modules::Module<mesos::master::allocator::Allocator> org_apache_mesos_GoHierarchicalAllocator(
  MESOS_MODULE_API_VERSION,
  MESOS_VERSION,
  "Apache Mesos",
  "modules@mesos.apache.org",
  "Go Hierarchical Allocator module.",
  NULL,
  createGoAllocator);

