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
#include <vector>

#include "encoder-decoder.hpp"
#include "clientlib.hpp"

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;

void ClientLib::virtual_read(
    const RowKey& row_key, iter_t slack) {
  OpInfo::OpType type = OpInfo::READ;
  OpInfo opinfo(type, row_key, slack);
  virtual_op(opinfo);
}

void ClientLib::virtual_write(const RowKey& row_key) {
  OpInfo opinfo(OpInfo::WRITE, row_key);
  virtual_op(opinfo);
}

void ClientLib::virtual_clock() {
  OpInfo opinfo(OpInfo::CLOCK);
  virtual_op(opinfo);
}

void ClientLib::virtual_op(const OpInfo& opinfo) {
  ThreadData& thread_data_ref = *thread_data;
  /* Record to operation sequence */
  thread_data_ref.opseq.push_back(opinfo);
}

void ClientLib::virtual_local_access(
    const RowKey& row_key) {
  ThreadData& thread_data_ref = *thread_data;
  LocalStorageIndex& local_storage_index = thread_data_ref.local_storage_index;
  if (!local_storage_index.count(row_key)) {
    local_storage_index[row_key] = thread_data_ref.local_storage_size++;
  }
}

void ClientLib::finish_virtual_iteration() {
  ThreadData& thread_data_ref = *thread_data;
  tbb::tick_count vi_start = tbb::tick_count::now();

  /* Summarize information */
  vi_thread_summarize();

  /* Set its virtual iteration state as finished gathering information */
  ScopedLock vilock(virtual_iteration_mutex);
  virtual_iteration_states[thread_data_ref.thread_id] = 11;
  virtual_iteration_cvar.notify_all();
  vilock.unlock();

  /* Wait for all other threads to finish gathering information from 
   * the virtual iteration */
  vilock.lock();
  while (clock_min(virtual_iteration_states) < 11) {
    virtual_iteration_cvar.wait(vilock);
  }
  vilock.unlock();

  vi_process_decisions();

  /* Set its virtual iteration state as finished making decisions */
  vilock.lock();
  virtual_iteration_states[thread_data_ref.thread_id] = 12;
  virtual_iteration_cvar.notify_all();
  vilock.unlock();

  /* Wait for all other threads to finish making decisions */
  vilock.lock();
  while (clock_min(virtual_iteration_states) < 12) {
    virtual_iteration_cvar.wait(vilock);
  }
  vilock.unlock();

  /* Make decisions based on the virtual iteration for the current thread */
  vi_thread_decisions();

  thread_data_ref.thread_stats.vi_time =
      (tbb::tick_count::now() - vi_start).seconds();
}

void ClientLib::vi_thread_summarize() {
  ThreadData& thread_data_ref = *thread_data;
  OpSeq& opseq = thread_data_ref.opseq;

  /* Construct merged vi info */
  uint clock = 0;
  std::vector<MergedViInfo> local_vi_infos(num_channels);
  for (uint i = 0; i < opseq.size(); i++) {
    OpInfo& opinfo = opseq[i];
    if (opinfo.type == OpInfo::CLOCK) {
      clock++;
      if (i == opseq.size() - 1) {
        break;
      }
    }
    uint channel_id = get_channel_id(opinfo.row_key);
    MergedViInfo& local_vi_info = local_vi_infos[channel_id];
    MergedViInfoRow& local_vi_info_row = local_vi_info[opinfo.row_key];
    /* Fill in merged vi info */
    if (opinfo.type != OpInfo::CLOCK) {
      local_vi_info_row.num_threads_access = 1;
    }
    if (opinfo.type == OpInfo::READ) {
      if (local_vi_info_row.clock_accessed.size() < clock + 1) {
        local_vi_info_row.clock_accessed.resize(clock + 1);
      }
      local_vi_info_row.clock_accessed[clock].read = true;
    }
    if (opinfo.type == OpInfo::WRITE) {
      if (local_vi_info_row.clock_accessed.size() < clock + 1) {
          local_vi_info_row.clock_accessed.resize(clock + 1);
        }
      local_vi_info_row.clock_accessed[clock].write = true;
    }
  }

  /* Merge info */
  uint start_channel = thread_data_ref.thread_id % num_channels;
  for (uint i = 0; i < num_channels; i++) {
    uint channel_id = (start_channel + i) % num_channels;
    MergedViInfo& local_vi_info = local_vi_infos[channel_id];
    CommunicationChannel& comm_channel = comm_channels[channel_id];
    ScopedLock channel_lock(*comm_channel.mutex);
    MergedViInfo& merged_vi_info = comm_channel.merged_vi_info;
    for (MergedViInfo::iterator iter = local_vi_info.begin();
         iter != local_vi_info.end(); iter++) {
      const RowKey& row_key = iter->first;
      MergedViInfoRow& local_vi_info_row = iter->second;
      MergedViInfoRow& merged_vi_info_row = merged_vi_info[row_key];
      merged_vi_info_row.num_threads_access +=
          local_vi_info_row.num_threads_access;
      ClockAccessed& local_clock_accessed = local_vi_info_row.clock_accessed;
      ClockAccessed& merged_clock_accessed = merged_vi_info_row.clock_accessed;
      if (merged_clock_accessed.size() < local_clock_accessed.size()) {
        merged_clock_accessed.resize(local_clock_accessed.size());
      }
      for (uint clock = 0; clock < local_clock_accessed.size(); clock++) {
        merged_clock_accessed[clock].read |=
          local_clock_accessed[clock].read;
        merged_clock_accessed[clock].write |=
          local_clock_accessed[clock].write;
      }
    }
  }

  /* Thread-0 will set num_vi_clocks for all channels.
   * We assume that all threads should have the same num_vi_clocks */
  if (thread_data_ref.thread_id == 0) {
    for (uint i = 0; i < comm_channels.size(); i++) {
      comm_channels[i].num_vi_clocks = clock;
    }
  }
}

void ClientLib::vi_process_decisions() {
  ThreadData& thread_data_ref = *thread_data;
  uint thread_id = thread_data_ref.thread_id;
  if (num_threads >= num_channels) {
    uint threads_per_channel = num_threads / num_channels;
    CHECK(threads_per_channel);
    bool is_leading = (thread_id % threads_per_channel == 0);
    if (!is_leading) {
      return;
    }
    uint channel_id = thread_id / threads_per_channel;
    if (channel_id >= num_channels) {
      return;
    }
    vi_process_channel_decisions(channel_id);
  } else {
    uint div = num_channels / num_threads;
    uint res = num_channels % num_threads;
    uint start = div * thread_id + (res > thread_id ? thread_id : res);
    uint size = div + (res > thread_id ? 1 : 0);
    for (uint channel_id = start; channel_id < start + size; channel_id++) {
      vi_process_channel_decisions(channel_id);
    }
  }
}

void ClientLib::vi_process_channel_decisions(uint channel_id) {
  ThreadData& thread_data_ref = *thread_data;
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCache& static_cache = comm_channel.static_cache;
  StaticCacheIndex& static_cache_index = static_cache.static_cache_index;
  StaticOpLog& oplog = static_cache.oplog;

  typedef vector<RowAccessInfo> AccessInfo;
  vector<AccessInfo> sharded_access_info(num_machines);
  uint row_count = 0;
  MergedViInfo& merged_vi_info = comm_channel.merged_vi_info;
  for (MergedViInfo::iterator iter = merged_vi_info.begin();
       iter != merged_vi_info.end(); iter++) {
    const RowKey& row_key = iter->first;
    MergedViInfoRow& merged_vi_info_row = iter->second;
    /* Set up static_cache_index */
    StaticCacheMetadata& static_cache_metadata =
        static_cache_index[row_key];
    static_cache_metadata.cache_idx = row_count;
    static_cache_metadata.server_id = -2;
    row_count++;
    /* Generate access info for deciding parameter placement */
    ClockAccessed& clock_accessed = merged_vi_info_row.clock_accessed;
    uint num_read = 0;
    uint num_write = 0;
    for (uint clock = 0; clock < clock_accessed.size(); clock++) {
      bool read = clock_accessed[clock].read;
      bool write = clock_accessed[clock].write;
      if (read) {
        /* There's a read in this clock */
        num_read++;
      }
      if (write) {
        /* There's a write in this clock */
        num_write++;
      }
    }
    uint machine_id = get_machine_id(row_key);
    AccessInfo& access_info = sharded_access_info[machine_id];
    access_info.push_back(RowAccessInfo());
    RowAccessInfo& access_info_entry = access_info.back();
    access_info_entry.row_key = row_key;
    access_info_entry.num_read = num_read;
    access_info_entry.num_write = num_write;
  }
  for (uint i = 0; i < sharded_access_info.size(); i++) {
    uint machine_id = i;
    comm_channel.encoder->report_access_info(machine_id, sharded_access_info[i]);
  }
  proc_stats.row_count += row_count;

  /* Create static cache */
  StaticDataCache& data_cache = static_cache.data_cache;
  data_cache.resize(row_count);
  static_cache.row_mutex_cvars.resize(row_count);
  for (int i = 0; i < row_count; i++) {
    static_cache.row_mutex_cvars[i] = make_shared<MutexCvar>();
  }

  /* Wait for find row */
  vector<uint> num_rows_in_server(num_machines);
  for (StaticCacheIndex::iterator iter = static_cache_index.begin();
       iter != static_cache_index.end(); iter++) {
    const RowKey& row_key = iter->first;
    StaticCacheMetadata& static_cache_metadata = iter->second;
    find_row_static_cache(row_key, channel_id);
    uint server_id =
        static_cast<uint>(static_cache_metadata.server_id);
    num_rows_in_server[server_id]++;
  }

  proc_stats.row_count_local += num_rows_in_server[machine_id];
  MultiserverRowKeys& oplog_rowkeys = oplog.oplog_rowkeys;
  oplog_rowkeys.resize(num_machines);
  oplog.oplog_entries.resize(MAX_CLOCK);
  vector<uint> cur_rows_in_server(num_machines);
  for (StaticCacheIndex::iterator iter = static_cache_index.begin();
       iter != static_cache_index.end(); iter++) {
    const RowKey& row_key = iter->first;
    StaticCacheMetadata& static_cache_metadata = static_cache_index[row_key];
    uint server_id = static_cast<uint>(static_cache_metadata.server_id);
    uint oplog_idx = cur_rows_in_server[server_id];
    static_cache_metadata.oplog_idx = oplog_idx;
    oplog_rowkeys[server_id].push_back(row_key);
    cur_rows_in_server[server_id]++;
  }
  /* Create oplog entries for the current clock */
  CHECK_GE(thread_data_ref.current_clock, 0);
  uint clock_idx = static_cast<uint>(thread_data_ref.current_clock);
  oplog.create_oplog_entry(clock_idx);

  /* Subscribe rows */
  CHECK_EQ(comm_channel.num_vi_clocks, 1);
  for (uint server_id = 0; server_id < num_machines; server_id++) {
    comm_channel.encoder->subscribe_rows(server_id, oplog_rowkeys[server_id]);
  }
}

void ClientLib::vi_thread_decisions() {
  ThreadData& thread_data_ref = *thread_data;
  OpSeq& opseq = thread_data_ref.opseq;

  /* Decide thread caching */
  ThreadCache& thr_cache = thread_data_ref.thr_cache;
  uint thr_cache_size = 0;
  ThreadCacheIndex& thr_cache_index = thread_data_ref.thr_cache_index;
  typedef graphlab::hopscotch_map<RowKey, uint> AccessCount;
  AccessCount access_count;
  size_t total_access = 0;
  for (uint i = 0; i < opseq.size(); i++) {
    OpInfo& opinfo = opseq[i];
    if (opinfo.type != OpInfo::CLOCK) {
      access_count[opinfo.row_key]++;
      total_access++;
    }
  }
  /* Cache entries with high contention probability */
  for (AccessCount::iterator iter = access_count.begin();
       iter != access_count.end(); iter++) {
    size_t num_access = iter->second;
    double access_freq =
        static_cast<double>(num_access) / total_access;
    const RowKey& row_key = iter->first;
    uint channel_id = get_channel_id(row_key);
    CommunicationChannel& comm_channel = comm_channels[channel_id];
    uint num_threads_access =
        comm_channel.merged_vi_info[row_key].num_threads_access;
    CHECK_LE(num_threads_access, num_threads);
    double contention_prob = access_freq * num_threads_access;
    if (contention_prob >= config.thread_cache_threshold) {
      /* Set up thread cache for this entry */
      uint row_idx = thr_cache_size;
      thr_cache_size++;
      if (thr_cache_size > thr_cache.size()) {
        if (thr_cache_size > thr_cache.capacity()) {
          thr_cache.reserve(get_nearest_power2(thr_cache_size));
        }
        thr_cache.resize(thr_cache_size);
      }
      thr_cache_index[row_key] = row_idx;
      ThreadCacheRow& thr_cache_row = thr_cache[row_idx];
      init_thr_cache_row(thr_cache_row, row_key);
    }
  }
  thread_data_ref.thread_stats.num_thread_cache = thr_cache_size;
  thread_data_ref.thread_stats.num_thread_cache_max = thr_cache_size;

  /* Create local storage */
  LocalStorage& local_storage = thread_data_ref.local_storage;
  local_storage.resize(thread_data_ref.local_storage_size);
}
