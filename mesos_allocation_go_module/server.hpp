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

#ifndef SERVER_HPP_
#define SERVER_HPP_

#include <httpserver/HTTPServer.h>

using namespace proxygen;

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class Server
{
private:
  std::unique_ptr<HTTPServer> server;
  std::unique_ptr<std::thread> server_thread;
public:
  Server();
  virtual ~Server();
  bool start();
  void stop();

private:
  virtual bool onStarting() = 0;
  virtual void initOptions(HTTPServerOptions& options) = 0;
};

class Handler : public proxygen::RequestHandler
{
private:
  std::string boundary;
  int content_length;
  std::unique_ptr<folly::IOBuf> body;

public:
  Handler();
  virtual ~Handler();

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers)
  noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

private:
  virtual void process(const std::string& type, const std::string& value) = 0;
};

}}}}



#endif /* SERVER_HPP_ */
