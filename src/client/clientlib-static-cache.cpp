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

#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>

#include <tbb/tick_count.h>

#include <string>
#include <utility>
#include <vector>

#include "encoder-decoder.hpp"
#include "clientlib.hpp"

using std::string;
using std::vector;
using std::cerr;
using std::cout;
using std::endl;
using boost::shared_ptr;


int ClientLib::find_row_static_cache(
      const RowKey& row_key, uint channel_id,
      bool unique_request, bool non_blocking, bool force_refresh) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCache& static_cache = comm_channel.static_cache;
  StaticCacheIndex& static_cache_index = static_cache.static_cache_index;
  StaticCacheIndex::iterator index_it = static_cache_index.find(row_key);
  if (index_it == static_cache_index.end()) {
    /* Fail to find at static cache */
    return -1;
  }

  StaticCacheMetadata& static_cache_metadata = index_it->second;
  int& server_id_ref = static_cache_metadata.server_id;
  ScopedLock index_lock(static_cache.mutex);

  if (force_refresh) {
    /* Erase the previously cache data location, re-find the row.
     * Probably don't need to do that unless we are migrating rows. */
    server_id_ref = -1;
  }
  if (server_id_ref >= 0) {
    return 0;
  }

  bool call_server = true;
  if (unique_request && server_id_ref == -2) {
    /* Request on the fly, don't send another one */
    call_server = false;
  }
  while (server_id_ref < 0) {
    if (call_server) {
      /* Mark that the request is on the fly */
      server_id_ref = -2;
      call_server = false;
      index_lock.unlock();  /* Release the lock while calling the server */
      uint machine_id = get_machine_id(row_key);
      RowKey row_key(row_key);
      comm_channel.encoder->find_row(row_key, machine_id);
      index_lock.lock();  /* Pick up the lock again */
    } else if (non_blocking) {
      /* Not waiting for the reply */
      break;
    } else {
      static_cache.cvar.wait(index_lock);
    }
  }
  return 0;
}

int ClientLib::find_row_cbk_static_cache(
      const RowKey& row_key, uint32_t server_id, uint channel_id) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCache& static_cache = comm_channel.static_cache;
  StaticCacheIndex& static_cache_index = static_cache.static_cache_index;
  StaticCacheIndex::iterator index_it = static_cache_index.find(row_key);
  if (index_it == static_cache_index.end()) {
    /* Fail to find at static cache */
    return -1;
  }

  StaticCacheMetadata& static_cache_metadata = index_it->second;
  int& server_id_ref = static_cache_metadata.server_id;
  ScopedLock rowlock(static_cache.mutex);
  server_id_ref = static_cast<int>(server_id);
  static_cache.cvar.notify_all();
  return 0;
}

void ClientLib::push_updates_static_cache(uint channel_id, iter_t clock) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCache& static_cache = comm_channel.static_cache;
  StaticOpLog& oplog = static_cache.oplog;
  if (oplog.oplog_entries.size() == 0) {
    comm_channel.encoder->clock_broadcast(clock);
  } else {
    MultiserverRowKeys& oplog_rowkeys = oplog.oplog_rowkeys;
    uint clock_uint = static_cast<uint>(clock);
    CHECK_LT(clock_uint, oplog.oplog_entries.size());
    OpLogEntry& oplog_entry = oplog.oplog_entries[clock_uint];
    CHECK_EQ(oplog_entry.size(), num_machines);
    CHECK_EQ(oplog_rowkeys.size(), num_machines);
    for (uint server_id = 0; server_id < num_machines; server_id++) {
      StaticUpdateCache& update_cache = oplog_entry[server_id];
      RowKeys& row_keys = oplog_rowkeys[server_id];
      CHECK_EQ(row_keys.size(), update_cache.row_updates.size());
      uint batch_size = row_keys.size();
      comm_channel.encoder->clock_with_updates_batch(
          server_id, clock, batch_size,
          row_keys.data(), update_cache.row_updates.data());
    }
  }
}

int ClientLib::recv_row_static_cache(
    uint channel_id, const RowKey& row_key,
    iter_t data_age, iter_t self_clock,
    const RowData& data, bool timing) {
  tbb::tick_count set_row_start;
  tbb::tick_count set_row_end;
  if (timing) {
    set_row_start = tbb::tick_count::now();
  }

  CommunicationChannel& comm_channel = comm_channels[channel_id];
  BgthreadStats& bgthread_stats = comm_channel.bgthread_stats;
  StaticCache& static_cache = comm_channel.static_cache;
  StaticCacheIndex& static_cache_index = static_cache.static_cache_index;
  StaticCacheIndex::iterator index_it = static_cache_index.find(row_key);
  if (index_it == static_cache_index.end()) {
    /* Fail to find at static cache */
    return -1;
  }

  StaticCacheMetadata& static_cache_metadata = index_it->second;
  uint cache_idx = static_cache_metadata.cache_idx;
  uint server_id =
      static_cast<uint>(static_cache_metadata.server_id);
  uint oplog_idx = static_cache_metadata.oplog_idx;
  StaticDataCache& data_cache = static_cache.data_cache;
  StaticDataCacheRow& data_cache_row = data_cache[cache_idx];
  shared_ptr<MutexCvar>& row_mutex_cvar =
      static_cache.row_mutex_cvars[cache_idx];
  ScopedLock rowlock(row_mutex_cvar->mutex);
  RowData& cached_data = data_cache_row.data;

  CHECK_LE(data_age, self_clock);

  /* Refresh the cached data */
  cached_data = data;

  data_cache_row.data_age = data_age;

  if (config.read_my_writes) {
    /* Apply oplogs for read-my-writes */
    StaticOpLog& oplog = static_cache.oplog;
    int64_t oplog_count = 0;
    /* TODO: we might need to grab a lock before accessing fast_clock.
     * A better way of doing that is to remember the update information
     * in the row struct */
    for (iter_t clock = self_clock + 1; clock <= fast_clock; clock++) {
      uint clock_uint = static_cast<uint>(clock);
      CHECK_LT(clock_uint, oplog.oplog_entries.size());
      OpLogEntry& oplog_entry = oplog.oplog_entries[clock];
      CHECK_LT(server_id, oplog_entry.size());
      CHECK_LT(oplog_idx, oplog_entry[server_id].row_updates.size());
      CHECK_LT(oplog_idx, oplog_entry[server_id].flags.size());
      if (oplog_entry[server_id].flags[oplog_idx]) {
        RowData& row_update = oplog_entry[server_id].row_updates[oplog_idx];
        cached_data += row_update;
        oplog_count++;
      }
    }
    bgthread_stats.set_row_num_apply_oplog += oplog_count;
  }

  row_mutex_cvar->cvar.notify_all();

  if (timing) {
    set_row_end = tbb::tick_count::now();
    bgthread_stats.total_set_row_time +=
      (set_row_end - set_row_start).seconds();
  }
  return 0;
}

int ClientLib::read_row_static_cache(
    RowData *buffer, const RowKey& row_key,
    iter_t clock, iter_t required_data_age,
    uint channel_id, iter_t *data_age_ptr, bool timing) {
  ThreadData& thread_data_ref = *thread_data;
  tbb::tick_count get_entry_start;
  tbb::tick_count get_lock_start;
  tbb::tick_count wait_miss_start;
  tbb::tick_count cache_copy_start;
  if (timing) {
    get_entry_start = tbb::tick_count::now();
  }

  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCache& static_cache = comm_channel.static_cache;
  StaticCacheIndex& static_cache_index = static_cache.static_cache_index;
  StaticCacheIndex::iterator index_it = static_cache_index.find(row_key);
  if (index_it == static_cache_index.end()) {
    /* Fail to find at static cache */
    return -1;
  }

  StaticCacheMetadata& static_cache_metadata = index_it->second;
  uint cache_idx = static_cache_metadata.cache_idx;
  StaticDataCache& data_cache = static_cache.data_cache;
  StaticDataCacheRow& data_cache_row = data_cache[cache_idx];
  shared_ptr<MutexCvar>& row_mutex_cvar =
      static_cache.row_mutex_cvars[cache_idx];
  ScopedLock rowlock(row_mutex_cvar->mutex);
  RowData& cached_data = data_cache_row.data;

  if (timing) {
    wait_miss_start = tbb::tick_count::now();
    thread_data_ref.thread_stats.read_get_entry_time +=
      (wait_miss_start - get_entry_start).seconds();
  }

  while (data_cache_row.data_age < required_data_age) {
    if (!row_mutex_cvar->cvar.timed_wait(rowlock,
          boost::posix_time::milliseconds(static_cast<uint>(TIMEOUT)))) {
      /* Read timeout */
      if (thread_data_ref.thread_id == 0) {
        cerr << "Read row time out!" << endl;
        cerr << "Client: " << machine_id
             << " Thread: " << thread_data_ref.thread_id
             // << " RowKey: " << row_key.row
             << " Clock: " << thread_data_ref.current_clock
             << " Data age: " << data_cache_row.data_age
             << " Required data age: " << required_data_age
             << " Channel : " << channel_id
             << std::endl;
        // CHECK(0);
      }
    }
  }

  if (timing) {
    cache_copy_start = tbb::tick_count::now();
    thread_data_ref.thread_stats.read_wait_miss_time +=
      (cache_copy_start - wait_miss_start).seconds();
  }

  if (buffer != NULL) {
    *buffer = cached_data;
  }
  *data_age_ptr = data_cache_row.data_age;

  return 0;
}

int ClientLib::update_row_static_cache(
      const RowKey& row_key, const RowData *update,
      uint channel_id, bool timing) {
  ThreadData& thread_data_ref = *thread_data;
  iter_t clock = thread_data_ref.current_clock;
  tbb::tick_count apply_proc_cache_start;
  if (timing) {
    apply_proc_cache_start = tbb::tick_count::now();
  }

  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCache& static_cache = comm_channel.static_cache;
  StaticCacheIndex& static_cache_index = static_cache.static_cache_index;
  StaticCacheIndex::iterator index_it = static_cache_index.find(row_key);
  if (index_it == static_cache_index.end()) {
    /* Fail to find at static cache */
    return -1;
  }

  StaticCacheMetadata& static_cache_metadata = index_it->second;
  uint cache_idx = static_cache_metadata.cache_idx;
  uint server_id = static_cast<uint>(static_cache_metadata.server_id);
  uint oplog_idx = static_cache_metadata.oplog_idx;
  StaticDataCache& data_cache = static_cache.data_cache;
  StaticOpLog& oplog = static_cache.oplog;
  StaticDataCacheRow& data_cache_row = data_cache[cache_idx];
  shared_ptr<MutexCvar>& row_mutex_cvar =
      static_cache.row_mutex_cvars[cache_idx];
  ScopedLock rowlock(row_mutex_cvar->mutex);
  uint clock_uint = static_cast<uint>(clock);
  CHECK_LT(clock_uint, oplog.oplog_entries.size());
  OpLogEntry& oplog_entry = oplog.oplog_entries[clock];
  CHECK_LT(server_id, oplog_entry.size());
  CHECK_LT(oplog_idx, oplog_entry[server_id].row_updates.size());
  CHECK_LT(oplog_idx, oplog_entry[server_id].flags.size());
  oplog_entry[server_id].flags[oplog_idx] = 1;
  RowData& row_update = oplog_entry[server_id].row_updates[oplog_idx];
  row_update += *update;

  if (config.read_my_writes) {
    RowData& cached_data = data_cache_row.data;
    cached_data += *update;
  }

  thread_data_ref.thread_stats.total_apply_proc_cache++;
  if (timing) {
    thread_data_ref.thread_stats.update_apply_proc_cache_time +=
        (tbb::tick_count::now() - apply_proc_cache_start).seconds();
  }
  return 0;
}

void ClientLib::create_shared_oplog_entries(iter_t clock) {
  for (uint channel_id = 0; channel_id < num_channels; channel_id++) {
    CommunicationChannel& comm_channel = comm_channels[channel_id];
    if (config.affinity) {
      set_mem_affinity(comm_channel.numa_node_id);
    }
    StaticCache& static_cache = comm_channel.static_cache;
    StaticOpLog& oplog = static_cache.oplog;
    if (oplog.oplog_rowkeys.size() == 0) {
      /* No oplog entries to create */
      CHECK_EQ(channel_id, 0);  /* Should discover that very soon */
      break;
    }
    uint clock_uint = static_cast<uint>(clock);
    oplog.create_oplog_entry(clock_uint);
  }
  /* Reset memory affinity */
  if (config.affinity) {
    set_mem_affinity(thread_data->numa_node_id);
  }
}
