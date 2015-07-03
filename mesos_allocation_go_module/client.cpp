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

#include "client.hpp"
#include <string>
#include <curlpp/Options.hpp>

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

Client::Client(const std::string& ihost, const std::string &iport)
{
  curlpp::options::Url url(ihost);
  request.setOpt(url);
  //TODO check conversion
  curlpp::options::Port port(stoi(iport));
  request.setOpt(port);
  request.setOpt(new cURLpp::Options::Verbose(true));
}

void Client::postData(const std::string& event_name, const google::protobuf::Message& event_data)
{
  std::cerr << "Sending proto\n";
  Forms formParts;
  std::string proto_code;
  event_data.SerializeToString(&proto_code);

  const std::string type_name("type");
  const std::string value_name("value");

  formParts.push_back(new curlpp::FormParts::Content(type_name, event_name));
  formParts.push_back(new curlpp::FormParts::Content(value_name, proto_code));

  request.setOpt(new curlpp::options::HttpPost(formParts));
  request.perform();
}

}}}}
