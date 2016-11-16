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

#include "encoder-decoder.hpp"
#include "clientlib.hpp"


void ClientLib::init_thr_cache_row(
    ThreadCacheRow& thr_cache_row, const RowKey& row_key) {
  thr_cache_row.init(row_key);
  /* Record the metadata of shared cache in the thread cache */
  uint channel_id = get_channel_id(row_key);
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCache& static_cache = comm_channel.static_cache;
  StaticCacheIndex& static_cache_index = static_cache.static_cache_index;
  StaticCacheIndex::iterator static_cache_index_it =
      static_cache_index.find(row_key);
  if (static_cache_index_it != static_cache_index.end()) {
    thr_cache_row.static_cache_metadata_ptr =
        &(static_cache_index_it->second);
  } else {
    thr_cache_row.static_cache_metadata_ptr = NULL;
  }
}

ThreadCacheRow *ClientLib::get_thr_cache(
    ThreadData& thread_data_ref, const RowKey& row_key) {
  ThreadCacheIndex& thr_cache_index = thread_data_ref.thr_cache_index;
  ThreadCache& thr_cache = thread_data_ref.thr_cache;
  ThreadCacheIndex::iterator map_it = thr_cache_index.find(row_key);
  if (map_it != thr_cache_index.end()) {
    uint row_idx = map_it->second;
    return &thr_cache[row_idx];
  } else {
    return NULL;
  }
}

void ClientLib::flush_thr_oplog_row(
    ThreadCacheRow& thread_cache_row, iter_t clock) {
  if (!thread_cache_row.op_flag) {
    return;
  }
  /* Flush to shared cache */
  uint channel_id = get_channel_id(thread_cache_row.row_key);
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  StaticCacheMetadata *static_cache_metadata_ptr =
      thread_cache_row.static_cache_metadata_ptr;
  if (static_cache_metadata_ptr != NULL) {
    uint cache_idx = static_cache_metadata_ptr->cache_idx;
    uint server_id =
        static_cast<uint>(static_cache_metadata_ptr->server_id);
    uint oplog_idx = static_cache_metadata_ptr->oplog_idx;

    StaticCache& static_cache = comm_channel.static_cache;
    StaticOpLog& oplog = static_cache.oplog;
    uint clock_idx = static_cast<uint>(clock);
    CHECK_LT(clock_idx, oplog.oplog_entries.size());
    OpLogEntry& oplog_entry = oplog.oplog_entries[clock_idx];
    StaticDataCache& data_cache = static_cache.data_cache;
    StaticDataCacheRow& data_cache_row = data_cache[cache_idx];
    shared_ptr<MutexCvar>& row_mutex_cvar =
        static_cache.row_mutex_cvars[cache_idx];
    ScopedLock rowlock(row_mutex_cvar->mutex);
    CHECK_LT(server_id, oplog_entry.size());
    CHECK_LT(oplog_idx, oplog_entry[server_id].row_updates.size());
    RowData& row_update = oplog_entry[server_id].row_updates[oplog_idx];
    row_update += thread_cache_row.update;
    CHECK_LT(oplog_idx, oplog_entry[server_id].flags.size());
    oplog_entry[server_id].flags[oplog_idx] = 1;
    if (config.read_my_writes) {
      RowData& cached_data = data_cache_row.data;
      cached_data += thread_cache_row.update;
    }
  } else {
    CHECK(0);
  }
  /* Clear update values */
  thread_cache_row.update.init();
  thread_cache_row.op_flag = 0;
}

void ClientLib::flush_thr_oplog(iter_t clock) {
  ThreadData& thread_data_ref = *thread_data;
  ThreadCache& thr_cache = thread_data_ref.thr_cache;
  for (uint i = 0; i < thr_cache.size(); i++) {
    flush_thr_oplog_row(thr_cache[i], clock);
  }
}
