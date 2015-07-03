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

#include <mesos/hook.hpp>
#include <mesos/mesos.hpp>
#include <mesos/module.hpp>

#include <mesos/module/hook.hpp>

#include "messages/messages.hpp"
#include "./client.hpp"
#include "./hook_server.hpp"
#include "./allocator.pb.h"

using namespace mesos;
using namespace allocator;

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class GoHook : public mesos::Hook
{
private:
  Client client;
  HookServer server;
public:
  GoHook(const std::string&, const std::string&);
  virtual ::Result<Labels> masterLaunchTaskLabelDecorator(
    const TaskInfo& taskInfo,
    const FrameworkInfo& frameworkInfo,
    const SlaveInfo& slaveInfo);

  virtual ::Result<Labels> slaveRunTaskLabelDecorator(
    const TaskInfo& taskInfo,
    const FrameworkInfo& frameworkInfo,
    const SlaveInfo& slaveInfo);

  virtual ::Result<Environment> slaveExecutorEnvironmentDecorator(
    const ExecutorInfo& executorInfo);

  virtual Try<Nothing> slaveRemoveExecutorHook(
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo);
};


GoHook::GoHook(const std::string& ihost, const std::string& iport)
:client(ihost, iport)
{

}

::Result<Labels> GoHook::masterLaunchTaskLabelDecorator(
  const TaskInfo& taskInfo,
  const FrameworkInfo& frameworkInfo,
  const SlaveInfo& slaveInfo)
{
  TaskInfo* copy_taskInfo = new TaskInfo(taskInfo);
  FrameworkInfo* copy_frmInfo = new FrameworkInfo(frameworkInfo);
  SlaveInfo* copy_slaveInfo = new SlaveInfo(slaveInfo);

  MasterLaunchTaskLabelDecorator proto;
  proto.set_allocated_taskinfo(copy_taskInfo);
  proto.set_allocated_frameworkinfo(copy_frmInfo);
  proto.set_allocated_slaveinfo(copy_slaveInfo);

  server.start();
  client.postData("MasterLaunchTaskLabelDecorator", proto);
  Labels labels = server.waitForMasterLaunchTaskLabelDecorator();
  server.stop();
  return labels;
}

::Result<Labels> GoHook::slaveRunTaskLabelDecorator(
  const TaskInfo& taskInfo,
  const FrameworkInfo& frameworkInfo,
  const SlaveInfo& slaveInfo)
{
  TaskInfo* copy_taskInfo = new TaskInfo(taskInfo);
  FrameworkInfo* copy_frmInfo = new FrameworkInfo(frameworkInfo);
  SlaveInfo* copy_slaveInfo = new SlaveInfo(slaveInfo);

  SlaveRunTaskLabelDecorator proto;
  proto.set_allocated_taskinfo(copy_taskInfo);
  proto.set_allocated_frameworkinfo(copy_frmInfo);
  proto.set_allocated_slaveinfo(copy_slaveInfo);

  server.start();
  client.postData("SlaveRunTaskLabelDecorator", proto);
  Labels labels = server.waitForSlaveRunTaskLabelDecorator();
  server.stop();
  return labels;
}

::Result<Environment> GoHook::slaveExecutorEnvironmentDecorator(
  const ExecutorInfo& executorInfo)
{
  ExecutorInfo* copy_exInfo = new ExecutorInfo(executorInfo);

  SlaveExecutorEnvironmentDecorator proto;
  proto.set_allocated_executorinfo(copy_exInfo);

  server.start();
  client.postData("SlaveExecutorEnvironmentDecorator", proto);
  Environment env = server.waitForSlaveExecutorEnvironmentDecorator();
  server.stop();
  return env;
}


// This hook locates the file created by environment decorator hook
// and deletes it.
Try<Nothing> GoHook::slaveRemoveExecutorHook(
  const FrameworkInfo& frameworkInfo,
  const ExecutorInfo& executorInfo)
{
  FrameworkInfo* copy_frmInfo = new FrameworkInfo(frameworkInfo);
  ExecutorInfo* copy_exInfo = new ExecutorInfo(executorInfo);

  SlaveRemoveExecutorHook proto;
  proto.set_allocated_frameworkinfo(copy_frmInfo);
  proto.set_allocated_executorinfo(copy_exInfo);

  server.start();
  client.postData("SlaveRemoveExecutorHook", proto);
  server.stop();
  return Nothing();
}

}
}
}
}


static mesos::Hook* createHook(const mesos::Parameters& parameters)
{
  std::string host = "localhost";
  std::string port = "4050";
  bool host_set = false, port_set = false;
  for (int i = 0; i != parameters.parameter_size(); ++i)
  {
    if (port_set && host_set)
      break;
    if (parameters.parameter(i).has_key() && parameters.parameter(i).key() == "host" && parameters.parameter(i).has_value())
    {
      host = parameters.parameter(i).value();
      host_set = true;
      continue;
    }
    if (parameters.parameter(i).has_key() && parameters.parameter(i).key() == "port" && parameters.parameter(i).has_value())
    {
      port = parameters.parameter(i).value();
      port_set = true;

    }
  }
  return new mesos::master::allocator::custom::GoHook(host, port);
}


// Declares a Hook module named 'org_apache_mesos_TestHook'.
mesos::modules::Module<mesos::Hook> org_apache_mesos_GoHook(
  MESOS_MODULE_API_VERSION,
  MESOS_VERSION,
  "Apache Mesos",
  "modules@mesos.apache.org",
  "Go Hook module.",
  NULL,
  createHook);

