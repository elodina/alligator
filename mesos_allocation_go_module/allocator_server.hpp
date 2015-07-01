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
  enum EEventType
  {
    ET_AddFramework,
    ET_RemoveFramework,
    ET_ActivateFramework,
    ET_DeactivateFramework,
    ET_AddSlave,
    ET_RemoveSlave,
    ET_UpdateSlave,
    ET_ActivateSlave,
    ET_DeactivateSlave,
    ET_UpdateWhitelist,
    ET_RequestResources,
    ET_UpdateAllocation,
    ET_RecoverResources,
    ET_ReviveOffers,
    ET_UpdateFramework,
    ET_Count
  };

private:
  HierarchicalDRFAllocator* allocator;
  static const std::map<std::string, EEventType> event_type;

public:
  explicit AllocatorHandler(HierarchicalDRFAllocator* allocator);
  virtual ~AllocatorHandler();

private:
  virtual void process(const std::string& type, const std::string& value) override;

  void addFramework(const std::string& data);
  void removeFramework(const std::string& data);
  void activateFramework(const std::string& data);
  void deactivateFramework(const std::string& data);
  void addSlave(const std::string& data);
  void removeSlave(const std::string& data);
  void updateSlave(const std::string& data);
  void activateSlave(const std::string& data);
  void deactivateSlave(const std::string& data);
  void updateWhitelist(const std::string& data);
  void requestResources(const std::string& data);
  void updateAllocation(const std::string& data);
  void recoverResources(const std::string& data);
  void reviveOffers(const std::string& data);
  void updateFramework(const std::string& data);
};

}
}
}
}

#endif /* ALLOCATOR_SERVER_HPP_ */
