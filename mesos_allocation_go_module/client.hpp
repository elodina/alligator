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

#ifndef CLIENT_HPP_
#define CLIENT_HPP_

#include <curlpp/Options.hpp>
#include <google/protobuf/message.h>
#include <curlpp/cURLpp.hpp>
#include "curlpp/Easy.hpp"

using namespace cURLpp;

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class Client
{
private:
  static  Cleanup curl_init;
  Easy request;

public:
  Client();
  void postData(const std::string& event_name, const google::protobuf::Message& event_data);
};

}}}}

#endif /* CLIENT_HPP_ */
