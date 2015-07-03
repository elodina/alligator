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
#include "server.hpp"
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

DEFINE_int32(http_port, 4050, "Port to listen on with HTTP protocol");
DEFINE_string(ip, "0.0.0.0", "IP/Hostname to bind to");
DEFINE_int32(threads, 0, "Number of threads to listen on. Numbers <= 0 "
  "will use the number of cores on this machine.");

Server::Server()
:server(nullptr)
{

}

Server::~Server()
{

}

bool Server::start()
{
  //google::InstallFailureSignalHandler();

  if (!onStarting())
    return false;

  std::vector<HTTPServer::IPConfig> IPs = {
    {SocketAddress(FLAGS_ip, FLAGS_http_port, true), Protocol::HTTP},
  };

  if (FLAGS_threads <= 0) {
    FLAGS_threads = sysconf(_SC_NPROCESSORS_ONLN);
    CHECK(FLAGS_threads > 0);
  }

  HTTPServerOptions options;
  options.threads = static_cast<size_t>(FLAGS_threads);
  initOptions(options);
  server.reset(new HTTPServer(std::move(options)));

  server->bind(IPs);

  // Start HTTPServer mainloop in a separate thread
  server_thread.reset(new std::thread([&] () {
    server->start();
  }));

  return true;
}

void Server::stop()
{
  if (server_thread)
    server->stop();
}

Handler::Handler()
: boundary("--")
, content_length(0)
{
}

Handler::~Handler()
{

}

void Handler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  std::cerr << "On request header called\n ";
  std::cerr << "Request headers are:\n";
  HTTPHeaders& http_headers = headers->getHeaders();
  http_headers.forEach([](const std::string &h, const std::string &v)
    {
    std::cerr << h.c_str() << " " << v.c_str() << "\n";
    });

  content_length = stoi(http_headers.rawGet("Content-Length"));
  std::cerr << "Content length is " << content_length << '\n';
  http_headers.forEachValueOfHeader(HTTP_HEADER_CONTENT_TYPE, [this](const std::string &v)->bool
    {
    std::string boundary_str("boundary=");
    int pos = v.find(boundary_str.c_str());
    if (pos != -1)
    {
      boundary += v.substr(pos + boundary_str.length());

    }
    return true;
    });
}

void Handler::onBody(std::unique_ptr<folly::IOBuf> ibody) noexcept {
  std::cerr << "On body called\n ";

  const uint8_t* data_begin = ibody->data();
  std::string str_body((const char*)data_begin, content_length);
  std::cerr << "Body data begin\n" << str_body << "Body data end\n";

  std::cerr << "Boundary is " << boundary.c_str() << '\n';

  //TODO use regex
  const char* suffix_part = "\r\n";

  const char* name_type = "name=\"type\"\r\n\r\n";
  size_t type_pos = str_body.find(name_type);
  size_t type_val_begin = type_pos + strlen(name_type);
  size_t type_val_end = str_body.find(boundary.c_str(), type_val_begin);
  std::string type_val = str_body.substr(type_val_begin, type_val_end-type_val_begin-strlen(suffix_part));
  std::cerr << "Type is " << type_val.c_str() << '\n';

  const char* name_value = "name=\"value\"\r\n\r\n";
  size_t value_pos = str_body.find(name_value);
  size_t value_val_begin = value_pos + strlen(name_value);
  size_t value_val_end = str_body.find(boundary.c_str(), value_val_begin);
  std::string value_val = str_body.substr(value_val_begin, value_val_end-value_val_begin-strlen(suffix_part));
  std::cerr << "Value is " << value_val << '\n';

  body = std::move(ibody);

  process(type_val, value_val);
}

void Handler::onEOM() noexcept {
  std::cerr << "On EOM called\n ";
  ResponseBuilder(downstream_)
  .status(200, "OK")
  .body(std::move(body))
  .sendWithEOM();
}

void Handler::onUpgrade(UpgradeProtocol protocol) noexcept {
  std::cerr << "On upgrade called\n ";
}

void Handler::requestComplete() noexcept {
  std::cerr << "On request complete called\n ";
  delete this;
}

void Handler::onError(ProxygenError err) noexcept {
  std::cerr << "On request error called\n ";
  delete this;
}

}}}}
