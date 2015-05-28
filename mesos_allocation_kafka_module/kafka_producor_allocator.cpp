
#include <memory>
#include <mesos/mesos.hpp>
#include <mesos/master/allocator.hpp>
#include <mesos/module.hpp>
#include <mesos/module/allocator.hpp>
#include <librdkafka/rdkafkacpp.h>

namespace mesos {
namespace master {
namespace allocator {

namespace convertor {

template<class Key, class Value>
void toString(std::string& str, const hashmap<Key, Value>& data);
template<class Key>
void toString(std::string& str, const hashset<Key>& data);
template<class Value>
void toString(std::string& str, const std::vector<Value>& data);
template<class Value>
void toString(std::string& str, const Option<Value>& data);
void toString(std::string& str, const Resources& data);
void toString(std::string& str, const google::protobuf::Message& data);
void toString(std::string& str, const std::string& data);

template<class Key, class Value>
void toString(std::string& str, const hashmap<Key, Value>& data)
{
  for (typename hashmap<Key, Value>::const_iterator it = data.cbegin(); it != data.cend(); ++it)
  {
    std::string keyProtoBufStr, valueProtoBufStr;
    toString(keyProtoBufStr, it->first);
    toString(valueProtoBufStr, it->second);
    str += "\nKey " + keyProtoBufStr + ", Value " + valueProtoBufStr;
  }
}

template<class Key>
void toString(std::string& str, const hashset<Key>& data)
{
  for (typename hashset<Key>::const_iterator it = data.cbegin(); it != data.cend(); ++it)
  {
    std::string keyProtoBufStr;
    toString(keyProtoBufStr, *it);
    str += "\nKey " + keyProtoBufStr;
  }
}

template<class Value>
void toString(std::string& str, const std::vector<Value>& data)
{
  for (size_t i = 0; i != data.size(); ++i)
  {
    std::string valueProtoBufStr;
    toString(valueProtoBufStr, data[i]);
    std::ostringstream counterStr;
    counterStr << "val " << i << valueProtoBufStr;
    str += counterStr.str();
  }
}

template<class Value>
void toString(std::string& str, const Option<Value>& data)
{
  if (data.isSome())
    toString(str, data.get());
}


void toString(std::string& str, const Resources& data)
{
  int resCounter = 0;
  str += "\nResources ";
  for (Resources::const_iterator it = data.begin(); it != data.end(); ++it, ++resCounter)
  {
    std::string resourceProtoBufStr;
    toString(resourceProtoBufStr, *it);
    std::ostringstream resCounterStr;
    resCounterStr << "res " << resCounter << resourceProtoBufStr;
    str += resCounterStr.str();
  }
}

void toString(std::string& str, const google::protobuf::Message& data)
{
  data.SerializeToString(&str);
}

void toString(std::string& str, const std::string& data)
{
  str = data;
}

}

//template <typename AllocatorProcess>
class KafkaProducerAllocator : public Allocator//public MesosAllocator<AllocatorProcess>
{
  //typedef MesosAllocator<AllocatorProcess> Ancestor;

private:
  std::unique_ptr<RdKafka::Producer> producer;
  std::unique_ptr<RdKafka::Conf> topic_conf;
  std::unique_ptr<RdKafka::Conf> producer_conf;

public:
  // Factory to allow for typed tests.
  static Try<mesos::master::allocator::Allocator*> create();

  ~KafkaProducerAllocator();

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
    const Option<hashset<std::string> >& whitelist);

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
    const Option<Filters>& filters);

  void reviveOffers(
    const FrameworkID& frameworkId);

private:
  void produce(const std::string& topic_str, const std::string& message_str);

private:
  KafkaProducerAllocator();
  KafkaProducerAllocator(const KafkaProducerAllocator&); // Not copyable.
  KafkaProducerAllocator& operator=(const KafkaProducerAllocator&); // Not assignable.
};

//template <typename AllocatorProcess>
Try<mesos::master::allocator::Allocator*>
KafkaProducerAllocator::create()
//KafkaProducerAllocator<AllocatorProcess>::create()
{
  LOG(INFO) << "Created kafka allocator\n ";
  mesos::master::allocator::Allocator* allocator =
    new KafkaProducerAllocator();
  //new KafkaProducerAllocator<AllocatorProcess>();
  return CHECK_NOTNULL(allocator);
}

//template <typename AllocatorProcess>
//KafkaProducerAllocator<AllocatorProcess>::KafkaProducerAllocator()
KafkaProducerAllocator::KafkaProducerAllocator()
{
  std::string errstr;
  std::string brokers = "localhost:9092";

  producer_conf.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  producer_conf->set("metadata.broker.list", brokers, errstr);
  topic_conf.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

  producer.reset(RdKafka::Producer::create(producer_conf.get(), errstr));
  if (!producer) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    throw std::runtime_error("Failed to create kafka producer");
  }
  std::cout << "% Created producer " << producer->name() << std::endl;
}


//template <typename AllocatorProcess>
//KafkaProducerAllocator<AllocatorProcess>::~KafkaProducerAllocator()
KafkaProducerAllocator::~KafkaProducerAllocator()
{

}

void KafkaProducerAllocator::initialize(
  const Duration& allocationInterval,
  const lambda::function<
  void(const FrameworkID&,
    const hashmap<SlaveID, Resources>&)>& offerCallback,
    const hashmap<std::string, mesos::master::RoleInfo>& roles)
{

}

//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::addFramework(
inline void KafkaProducerAllocator::addFramework(
  const FrameworkID& frameworkId,
  const FrameworkInfo& frameworkInfo,
  const hashmap<SlaveID, Resources>& used)
{
  std::string frameworkIdProtoBufStr, frameworkInfoProtoBufStr, usedProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);
  convertor::toString(frameworkInfoProtoBufStr, frameworkInfo);
  convertor::toString(usedProtoBufStr, used);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr +
    "\nFrameworkInfo: " + frameworkInfoProtoBufStr +
    "\nUsed Key-SlaveID, Value-Resources: " + usedProtoBufStr;

  produce("Add_framework", line);
  //Ancestor::addFramework(frameworkId, frameworkInfo, used);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::removeFramework(
inline void KafkaProducerAllocator::removeFramework(
  const FrameworkID& frameworkId)
{
  std::string frameworkIdProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr;
  produce("Remove_framework", line);

  //Ancestor::removeFramework(frameworkId);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::activateFramework(
inline void KafkaProducerAllocator::activateFramework(
  const FrameworkID& frameworkId)
{
  std::string frameworkIdProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr;
  produce("Activate_framework", line);

  // Ancestor::activateFramework(frameworkId);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::deactivateFramework(
inline void KafkaProducerAllocator::deactivateFramework(
  const FrameworkID& frameworkId)
{
  std::string frameworkIdProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr;
  produce("Deactivate_framework", line);

  //Ancestor::deactivateFramework(frameworkId);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::addSlave(
inline void KafkaProducerAllocator::addSlave(
  const SlaveID& slaveId,
  const SlaveInfo& slaveInfo,
  const Resources& total,
  const hashmap<FrameworkID, Resources>& used)
{
  std::string slaveIdProtoBufStr, slaveInfoProtoBufStr, totalProtoBufStr, usedProtoBufStr;
  convertor::toString(slaveIdProtoBufStr, slaveId);
  convertor::toString(slaveInfoProtoBufStr, slaveInfo);
  convertor::toString(totalProtoBufStr, total);
  convertor::toString(usedProtoBufStr, used);

  std::string line = "SlaveID: " + slaveIdProtoBufStr +
    "\nSlaveInfo: " + slaveInfoProtoBufStr +
    "\nTotal: " + totalProtoBufStr +
    "\nUsed Key-FrameworkID, Value-Resources: " + usedProtoBufStr;

  produce("Add_slave", line);
  //Ancestor::addSlave(slaveId, slaveInfo, total, used);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::removeSlave(
inline void KafkaProducerAllocator::removeSlave(
  const SlaveID& slaveId)
{
  std::string slaveIdProtoBufStr;
  convertor::toString(slaveIdProtoBufStr, slaveId);

  std::string line = "SlaveID: " + slaveIdProtoBufStr;
  produce("Remove_slave", line);

  //Ancestor::removeSlave(slaveId);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::updateSlave(
inline void KafkaProducerAllocator::updateSlave(
  const SlaveID& slaveId,
  const Resources& oversubscribed)
{
  std::string slaveIdProtoBufStr, oversubscribedProtoBufStr;
  convertor::toString(slaveIdProtoBufStr, slaveId);
  convertor::toString(oversubscribedProtoBufStr, oversubscribed);

  std::string line = "SlaveID: " + slaveIdProtoBufStr +
    "\nOversubscribed: " + oversubscribedProtoBufStr;
  produce("Update_slave", line);

  //Ancestor::updateSlave(slaveId, oversubscribed);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::activateSlave(
inline void KafkaProducerAllocator::activateSlave(
  const SlaveID& slaveId)
{
  std::string slaveIdProtoBufStr;
  convertor::toString(slaveIdProtoBufStr, slaveId);

  std::string line = "SlaveID: " + slaveIdProtoBufStr;
  produce("Activate_slave", line);

  //Ancestor::activateSlave(slaveId);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::deactivateSlave(
inline void KafkaProducerAllocator::deactivateSlave(
  const SlaveID& slaveId)
{
  std::string slaveIdProtoBufStr;
  convertor::toString(slaveIdProtoBufStr, slaveId);

  std::string line = "SlaveID: " + slaveIdProtoBufStr;
  produce("Deactivate_slave", line);

  //Ancestor::deactivateSlave(slaveId);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::updateWhitelist(
inline void KafkaProducerAllocator::updateWhitelist(
  const Option<hashset<std::string> >& whitelist)
{
  std::string whiteListProtoBufStr;
  convertor::toString(whiteListProtoBufStr, whitelist);

  std::string line = "Whitelist: " + whiteListProtoBufStr;
  produce("Update_whitelist", line);

  //Ancestor::updateWhitelist(whitelist);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::requestResources(
inline void KafkaProducerAllocator::requestResources(
  const FrameworkID& frameworkId,
  const std::vector<Request>& requests)
{
  std::string frameworkIdProtoBufStr, requestsProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);
  convertor::toString(requestsProtoBufStr, requests);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr +
    "\nRequests: " + requestsProtoBufStr;
  produce("Request_resources", line);

  //Ancestor::requestResources(frameworkId, requests);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::updateAllocation(
inline void KafkaProducerAllocator::updateAllocation(
  const FrameworkID& frameworkId,
  const SlaveID& slaveId,
  const std::vector<Offer::Operation>& operations)
{

  std::string frameworkIdProtoBufStr, slaveIdProtoBufStr, operationsProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);
  convertor::toString(slaveIdProtoBufStr, slaveId);
  convertor::toString(operationsProtoBufStr, operations);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr +
    "SlaveID: " + slaveIdProtoBufStr +
    "\nOperations: " + operationsProtoBufStr;
  produce("Update_allocation", line);

  //Ancestor::updateAllocation(frameworkId, slaveId, operations);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::recoverResources(
inline void KafkaProducerAllocator::recoverResources(
  const FrameworkID& frameworkId,
  const SlaveID& slaveId,
  const Resources& resources,
  const Option<Filters>& filters)
{
  std::string frameworkIdProtoBufStr, slaveIdProtoBufStr, resourcesProtoBufStr, filtersProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);
  convertor::toString(slaveIdProtoBufStr, slaveId);
  convertor::toString(resourcesProtoBufStr, resources);
  convertor::toString(filtersProtoBufStr, filters);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr +
    "\nSlaveID: " + slaveIdProtoBufStr +
    "\nResources: " + resourcesProtoBufStr +
    "\nFilters: " + filtersProtoBufStr;
  produce("Recover_resources", line);

  //Ancestor::recoverResources(frameworkId, slaveId, resources, filters);
}


//template <typename AllocatorProcess>
//inline void KafkaProducerAllocator<AllocatorProcess>::reviveOffers(
inline void KafkaProducerAllocator::reviveOffers(
  const FrameworkID& frameworkId)
{
  std::string frameworkIdProtoBufStr;
  convertor::toString(frameworkIdProtoBufStr, frameworkId);

  std::string line = "FrameworkID: " + frameworkIdProtoBufStr;
  produce("Revive_offers", line);

  //Ancestor::reviveOffers(frameworkId);
}

void KafkaProducerAllocator::produce(const std::string& topic_str, const std::string& message_str) {

  /*
   * Create topic handle.
   */
  std::string errstr;
  int32_t partition = RdKafka::Topic::PARTITION_UA;
  std::unique_ptr<RdKafka::Topic> topic(RdKafka::Topic::create(producer.get(), topic_str.c_str(),topic_conf.get(), errstr));
  if (!topic) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    return;
  }

  RdKafka::ErrorCode resp =
    producer->produce(topic.get(), partition,
      RdKafka::Producer::RK_MSG_COPY,
      const_cast<char *>(message_str.c_str()), message_str.size(),
      NULL, NULL);
  if (resp != RdKafka::ERR_NO_ERROR)
    std::cerr << "% Produce failed: " <<
    RdKafka::err2str(resp) << std::endl;
  else
    std::cerr << "% Produced message ( line = " << message_str.c_str() << message_str.size() << " bytes)" <<
    std::endl;
  producer->poll(0);
}

}
}
}

static mesos::master::allocator::Allocator* createKafkaAllocator(const mesos::Parameters& parameters)
{
  return mesos::master::allocator::KafkaProducerAllocator::create().get();
}

mesos::modules::Module<mesos::master::allocator::Allocator> org_apache_mesos_KafkaHierarchicalAllocator(
  MESOS_MODULE_API_VERSION,
  MESOS_VERSION,
  "Apache Mesos",
  "modules@mesos.apache.org",
  "Kafka Hierarchical Allocator module.",
  NULL,
  createKafkaAllocator);

