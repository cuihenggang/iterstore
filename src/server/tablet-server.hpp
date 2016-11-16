#ifndef __tablet_server_hpp__
#define __tablet_server_hpp__

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

/* Tablet server */

#include <tbb/tick_count.h>

#include <boost/make_shared.hpp>
#include <boost/lexical_cast.hpp>

#include <fstream>
#include <vector>
#include <string>
#include <utility>
#include <set>
#include <map>

#include "common/common-utils.hpp"
#include "common/router-handler.hpp"
#include "common/hopscotch_map.hpp"
#include "common/stats-tracker.hpp"

using std::string;
using std::vector;

using boost::shared_ptr;

class ServerClientEncode;
class MetadataServer;   /* Used in get_stats() */

class TabletStorage {
 public:
  ServerStats server_stats;

  typedef boost::unordered_map<RowKey, uint> Row2Index;

  struct RowStorage {
    bool initted;
    RowKey row_key;
    RowData data;
    RowStorage() : initted(false) {}
    void init(const RowKey& row_key) {
      if (initted) {
        return;
      }
      this->row_key = row_key;
      initted = true;
    }
    // template<class Archive>
    // void serialize(Archive & ar, const unsigned int version) {
      // ar & initted;
      // ar & row_key;
      // ar & data;
    // }
  };
  typedef std::vector<RowStorage> TableStorage;
  struct ModelStorage {
    TableStorage storage;
    TableStorage accum_gradients_store;
    TableStorage old_accum_gradients_store;
    TableStorage z_store;
    TableStorage z_max_store;
  };

  struct SubscribedReads {
    std::vector<RowKey> row_keys;
    std::vector<RowData> row_data_batch;

    SubscribedReads() {}
    SubscribedReads(size_t size)
        : row_keys(size), row_data_batch(size) {}
    void resize(size_t size) {
      row_keys.resize(size);
      row_data_batch.resize(size);
    }
    void reserve(size_t size) {
      row_keys.reserve(size);
      row_data_batch.reserve(size);
    }
    size_t size() {
      CHECK_EQ(row_keys.size(), row_data_batch.size());
      return row_keys.size();
    }
    size_t capacity() {
      CHECK_EQ(row_keys.capacity(), row_data_batch.capacity());
      return row_keys.capacity();
    }
  };
  typedef std::vector<SubscribedReads> AllClientSubscribedReads;

 private:
  uint channel_id;
  uint num_channels;
  uint machine_id;
  uint num_machines;
  uint client_count;

  uint row_count;

  shared_ptr<ServerClientEncode> communicator;

  VecClock worker_started_states;
  iter_t all_workers_started;
  DoubleVec client_progresses;

  VecClock vec_clock;
  iter_t global_clock;

  Row2Index row2idx;
  ModelStorage model_storage;
  AllClientSubscribedReads all_client_subscribed_reads;

  Config config;

  tbb::tick_count start_time;
  tbb::tick_count first_come_time;

 private:
  template<class T>
  void resize_storage(vector<T>& storage, uint size) {
    if (storage.capacity() <= size) {
      uint capacity = get_nearest_power2(size);
      storage.reserve(capacity);
    }
    if (storage.size() <= size) {
      storage.resize(size);
    }
  }
  uint get_row_idx(const RowKey& row_key);
  void send_read_batch(uint client_id, RowKeys& row_keys);
  void process_all_client_subscribed_reads(
      iter_t clock);
  void process_subscribed_reads(
      uint client_id, iter_t data_age,
      SubscribedReads& subscribed_reads);

 public:
  TabletStorage(
      uint channel_id, uint num_channels,
      uint machine_id, uint num_machines,
      shared_ptr<ServerClientEncode> comm,
      const Config& config);
  void update_row_batch(
      uint client_id, iter_t clock,
      uint batch_size, RowKey *row_keys, RowData *row_updates);
  void apply_updates_blank(
      const RowData& update_row, uint row_idx,
      const Tunable& tunable);
  void apply_updates_stepsize(
      const RowData& update_row, uint row_idx,
      const Tunable& tunable);
  void apply_updates_adagrad(
      const RowData& update_row, uint row_idx,
      const Tunable& tunable);
  void apply_updates_adarevision(
      const RowData& update_row, uint row_idx,
      const Tunable& tunable);
  void clock(uint32_t client_id, iter_t clock);
  void subscribe_rows(uint client_id, RowKeys& row_keys);
  void get_stats(uint client_id, shared_ptr<MetadataServer> metadata_server);
  void worker_started(uint client_id);
  void report_progress(uint client_id, int clock, double progress);
};

#endif  // defined __tablet_server_hpp__
