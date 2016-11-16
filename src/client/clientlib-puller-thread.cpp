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

#include <tbb/tick_count.h>

#include <string>
#include <utility>
#include <vector>

#include "common/work-pusher.hpp"
#include "common/background-worker.hpp"
#include "encoder-decoder.hpp"
#include "clientlib.hpp"

using std::string;
using std::vector;
using std::cerr;
using std::cout;
using std::endl;


void ClientLib::find_row_cbk(
    const RowKey& row_key, uint32_t server_id) {
  uint channel_id = get_channel_id(row_key);
  int ret = find_row_cbk_static_cache(row_key, server_id, channel_id);
  CHECK_EQ(ret, 0);
}

void ClientLib::recv_row_batch(
    uint channel_id, iter_t data_age, iter_t self_clock,
    RowKey *row_keys, RowData *row_data, uint batch_size) {
  BgthreadStats& bgthread_stats = comm_channels[channel_id].bgthread_stats;
  tbb::tick_count set_row_start;
  tbb::tick_count set_row_end;
  int timing = true;
  if (timing) {
    set_row_start = tbb::tick_count::now();
  }

  // cout << "Client " << machine_id << " received row of clock "
       // << data_age << " with size " << batch_size
       // << " from channel " << channel_id
       // << endl;

  for (uint i = 0; i < batch_size; i++) {
    /* Look at static cache */
    int ret = recv_row_static_cache(
        channel_id, row_keys[i], data_age, self_clock, row_data[i], false);
    CHECK_EQ(ret, 0);
  }

  if (timing) {
    set_row_end = tbb::tick_count::now();
    bgthread_stats.total_set_row_time +=
      (set_row_end - set_row_start).seconds() / SET_ROW_TIMING_FREQ;
  }
}

void ClientLib::server_iterate(
        uint channel_id, uint tablet_id, iter_t clock) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  CHECK_LT(tablet_id, comm_channel.server_clock.size());
  comm_channel.server_clock[tablet_id] = clock;

  iter_t min_clock = clock_min(comm_channel.server_clock);
  if (min_clock > comm_channel.shared_iteration) {
    comm_channel.shared_iteration = min_clock;
    /* Remove oplog entries.
     * We don't need to grab any locks,
     * because there shouldn't be be any threads using it. */
    uint clock_uint = static_cast<uint>(min_clock);
    comm_channel.static_cache.oplog.free_oplog_entry(clock_uint);
  }
}

void ClientLib::server_started(uint channel_id, uint server_id) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  ScopedLock channel_lock(*comm_channel.mutex);
  CHECK_LT(server_id, comm_channel.server_started.size());
  comm_channel.server_started[server_id] = 1;
  comm_channel.all_server_started = clock_min(comm_channel.server_started);
  comm_channel.cvar->notify_all();
}

void ClientLib::get_stats_cbk(const string& server_stats) {
  ScopedLock server_stats_lock(server_stats_mutex);
  proc_stats.server_stats = server_stats;
  proc_stats.server_stats_refreshed = true;
  server_stats_cvar.notify_all();
}
