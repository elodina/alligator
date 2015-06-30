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
  Labels slave_run_task_label_decorator;

public:
  HookServer();
  ~HookServer();

  Labels waitForSlaveRunTaskLabelDecorator();

private:
  virtual bool onStarting() override;
  virtual void initOptions(HTTPServerOptions& options) override;
};

class HookHandler : public Handler
{
private:
  std::function<void(const std::string&)> onWaitForSlaveRunTaskLabelDecorator;

public:
  HookHandler(std::function<void(const std::string&)> fSlaveRunTaskLabelDecorator);
  virtual ~HookHandler();

private:
  virtual void process(const std::string& type, const std::string& value) override;
};

}
}
}
}




#endif /* HOOK_SERVER_HPP_ */
