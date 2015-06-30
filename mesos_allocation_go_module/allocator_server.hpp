#ifndef ALLOCATOR_SERVER_HPP_
#define ALLOCATOR_SERVER_HPP_

#include <master/allocator/mesos/hierarchical.hpp>
#include "./server.hpp"

using namespace mesos::internal::master::allocator;

namespace mesos {
namespace master {
namespace allocator {
namespace custom {

class AllocatorServer : public Server
{
private:
  HierarchicalDRFAllocator* allocator;

public:
  AllocatorServer();
  virtual ~AllocatorServer();
  HierarchicalDRFAllocator* getAllocator();

private:
  virtual bool onStarting() override;
  virtual void initOptions(HTTPServerOptions& options) override;
};

class AllocatorHandler : public Handler
{
private:
  HierarchicalDRFAllocator* allocator;

public:
  explicit AllocatorHandler(HierarchicalDRFAllocator* allocator);
  virtual ~AllocatorHandler();

private:
  virtual void process(const std::string& type, const std::string& value) override;
  void addSlave(const std::string& data);
  void addFramework(const std::string& data);
};

}
}
}
}

#endif /* ALLOCATOR_SERVER_HPP_ */
