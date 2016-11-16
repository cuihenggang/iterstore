#ifndef __app_background_worker_hpp__
#define __app_background_worker_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <boost/function.hpp>
#include <boost/unordered_map.hpp>
#include <boost/make_shared.hpp>
#include <boost/bind.hpp>

#include <vector>
#include <string>

#include "include/lazytable-user-defined-types.hpp"
#include "client/work-puller.hpp"

class AppBackgroundWorker {
 public:
  explicit AppBackgroundWorker(
      const boost::shared_ptr<WorkPuller>& work_puller_i)
    : work_puller(work_puller_i) {}
  typedef boost::function<void (std::vector<ZmqPortableBytes>&)>
    WorkerCallback;
  static const uint32_t STOP_CMD = 0;
  int add_callback(uint32_t cmd, WorkerCallback callback) {
    if (cmd == STOP_CMD) {
      return -1;
    }
    if (callback_map.count(cmd)) {
      return -1;
    }

    callback_map[cmd] = callback;
    return 0;
  }

  void pull_work_loop() {
    while (1) {
      uint32_t cmd = 0;
      std::vector<ZmqPortableBytes> args;
      int ret = work_puller->pull_work(cmd, args);
      if (ret < 0 || cmd == 0) {
        break;
      }
      if (!callback_map.count(cmd)) {
        std::cerr << "Received unknown command!" << std::endl;
        assert(0);
      }
      WorkerCallback& callback = callback_map[cmd];
      callback(args);
    }
  }

  /* This function can used as the entry point of a boost thread */
  void operator()() {
    pull_work_loop();
  }

 private:
  boost::shared_ptr<WorkPuller> work_puller;
  boost::unordered_map<uint32_t, WorkerCallback> callback_map;
};

#endif  // defined __app_background_worker_hpp__
