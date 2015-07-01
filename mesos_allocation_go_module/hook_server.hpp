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

#ifndef HOOK_SERVER_HPP_
#define HOOK_SERVER_HPP_

#include <mutex>
#include <condition_variable>
#include <functional>
#include "./server.hpp"
#include "./allocator.pb.h"

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class HookServer : public Server
{
private:
  std::mutex mtx;
  std::condition_variable cv;
  bool ready;
  Labels master_launch_task_label_decorator;
  Labels slave_run_task_label_decorator;
  Environment slave_executor_environment_decorator;

public:
  HookServer();
  ~HookServer();

  Labels waitForSlaveRunTaskLabelDecorator();
  Labels waitForMasterLaunchTaskLabelDecorator();
  Environment waitForSlaveExecutorEnvironmentDecorator();

private:
  virtual bool onStarting() override;
  virtual void initOptions(HTTPServerOptions& options) override;
  void notifyReady();
};

class HookHandler : public Handler
{
private:
  std::function<void(const std::string&)> onWaitForMasterLaunchTaskLabelDecorator;
  std::function<void(const std::string&)> onWaitForSlaveRunTaskLabelDecorator;
  std::function<void(const std::string&)> onWaitForSlaveExecutorEnvironmentDecorator;

public:
  HookHandler(std::function<void(const std::string&)> fMasterLaunchTaskLabelDecorator,
    std::function<void(const std::string&)> fSlaveRunTaskLabelDecorator,
    std::function<void(const std::string&)> fSlaveExecutorEnvironmentDecorator);
  virtual ~HookHandler();

private:
  virtual void process(const std::string& type, const std::string& value) override;
};

}
}
}
}




#endif /* HOOK_SERVER_HPP_ */
