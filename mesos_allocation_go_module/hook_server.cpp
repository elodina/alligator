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
#include "hook_server.hpp"
#include <gflags/gflags.h>
#include <folly/Memory.h>
#include <folly/Portability.h>
#include <folly/io/async/EventBaseManager.h>
#include <httpserver/ResponseBuilder.h>
#include <unistd.h>

using namespace allocator;

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

using Protocol = HTTPServer::Protocol;

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class HookHandlerFactory : public RequestHandlerFactory {
public:
  explicit HookHandlerFactory(std::function<void(const std::string&)> fSlaveRunTaskLabelDecorator)
  : onWaitForSlaveRunTaskLabelDecorator(fSlaveRunTaskLabelDecorator)
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
    return new HookHandler(onWaitForSlaveRunTaskLabelDecorator);
    }

private:
  std::function<void(const std::string&)> onWaitForSlaveRunTaskLabelDecorator;
};

HookServer::HookServer()
:ready(false)
{

}

HookServer::~HookServer()
{

}

void HookServer::initOptions(HTTPServerOptions& options)
{
  options.idleTimeout = std::chrono::milliseconds(60000);
  options.shutdownOn = {SIGINT, SIGTERM};
  options.enableContentCompression = true;
  std::unique_ptr<RequestHandlerFactory> factory(new HookHandlerFactory([this](const std::string& value)->void
    {
    slave_run_task_label_decorator.ParseFromString(value);
    std::unique_lock<std::mutex> lck(mtx);
    ready = true;
    cv.notify_all();
    }));

  options.handlerFactories = RequestHandlerChain()
                  .addThen(factory)
                  .build();
}

bool HookServer::onStarting()
{
  return true;
}

Labels HookServer::waitForSlaveRunTaskLabelDecorator()
{
  std::unique_lock<std::mutex> lck(mtx);
  bool info_displayed = false;
  while (!ready)
  {
    if (!info_displayed)
    {
      std::cerr << "Waiting for labels from Go server...\n";
      info_displayed = true;
    }
    cv.wait(lck);
  }
  std::cerr << "Received labels from Go server\n";
  return slave_run_task_label_decorator;
}

HookHandler::HookHandler(std::function<void(const std::string&)> fSlaveRunTaskLabelDecorator)
: onWaitForSlaveRunTaskLabelDecorator(fSlaveRunTaskLabelDecorator)
{

}

HookHandler::~HookHandler()
{

}

void HookHandler::process(const std::string& type, const std::string& value)
{
  if (type == "SlaveRunTaskLabelDecorator")
    onWaitForSlaveRunTaskLabelDecorator(value);
}

}}}}
