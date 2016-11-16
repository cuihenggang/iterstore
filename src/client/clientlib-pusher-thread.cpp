/*
 * Copyright (c) 2016, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <string>
#include <utility>
#include <vector>

#include <tbb/tick_count.h>

#include "common/work-pusher.hpp"
#include "common/background-worker.hpp"
#include "encoder-decoder.hpp"
#include "clientlib.hpp"

using std::string;
using std::vector;
using std::cerr;
using std::cout;
using std::endl;


void ClientLib::signal_bg_work__worker_started(uint channel_id) {
  comm_channels[channel_id].work_pusher->push_work(WORKER_STARTED_CMD);
}

void ClientLib::perform_bg_work__worker_started(
        uint channel_id, vector<ZmqPortableBytes>& args) {
  worker_started(channel_id);
  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientLib::worker_started(uint channel_id) {
  /* Wait for the server to reply the worker started message */
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  ScopedLock channel_lock(*comm_channel.mutex);
  comm_channel.encoder->worker_started();
  while (!comm_channel.all_server_started) {
    if (!comm_channel.cvar->timed_wait(channel_lock,
        boost::posix_time::milliseconds(2000))) {
      /* Resend the worker started message */
      comm_channel.encoder->worker_started();
    }
  }
}

void ClientLib::signal_bg_work__push_updates(
    uint channel_id, iter_t clock, double progress) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);
  msgs[0].init_size(sizeof(bgcomm_clock_msg_t));
  bgcomm_clock_msg_t *clock_msg =
    reinterpret_cast<bgcomm_clock_msg_t *>(msgs[0].data());
  clock_msg->clock = clock;
  clock_msg->progress = progress;
  comm_channels[channel_id].work_pusher->push_work(PUSH_UPDATES_CMD, msgs);
}

void ClientLib::perform_bg_work__push_updates(
    uint channel_id, vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  bgcomm_clock_msg_t *clock_msg =
      reinterpret_cast<bgcomm_clock_msg_t *>(args[0].data());
  push_updates(channel_id, clock_msg->clock, clock_msg->progress);
  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientLib::push_updates(
    uint channel_id, iter_t clock, double progress) {
  int timing = true;
  tbb::tick_count push_updates_start;
  tbb::tick_count push_updates_end;

  if (timing) {
    push_updates_start = tbb::tick_count::now();
  }

  CommunicationChannel& comm_channel = comm_channels[channel_id];
  /* There's only one push_updates() threads for each channel,
   * so we don't need to grab the channel lock */
  BgthreadStats& bgthread_stats = comm_channel.bgthread_stats;
  CHECK_EQ(clock, comm_channel.update_sending_iter);

  /* Report progress */
  comm_channel.encoder->report_progress(clock, progress);

  /* Push updates */
  push_updates_static_cache(channel_id, clock);
  comm_channel.update_sending_iter++;

  if (timing) {
    push_updates_end = tbb::tick_count::now();
    bgthread_stats.total_push_updates_time +=
      (push_updates_end - push_updates_start).seconds();
  }
}
