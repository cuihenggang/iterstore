#ifndef __lazytable_impl_hpp__
#define __lazytable_impl_hpp__

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

/* Implementation of Client library */

#include <zmq.hpp>

#include <tbb/tick_count.h>

#include <boost/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include <set>
#include <string>
#include <vector>
#include <map>
#include <utility>

#include "common/router-handler.hpp"
#include "common/stats-tracker.hpp"
#include "common/work-pusher.hpp"
#include "server/server-entry.hpp"


struct StaticDataCacheRow {
  RowData data;
  iter_t data_age;
  void init() {
    data_age = UNINITIALIZED_CLOCK;
  }
  StaticDataCacheRow() {
    init();
  }
};
typedef std::vector<StaticDataCacheRow> StaticDataCache;

struct StaticUpdateCache {
  RowDatas row_updates;
  vector<int> flags;
};
typedef std::vector<StaticUpdateCache> MultiserverStaticUpdateCache;
    /* Indexed by server-id */
typedef MultiserverStaticUpdateCache OpLogEntry;
typedef std::vector<OpLogEntry> OpLogEntries;
    /* Indexed by clock */
typedef std::vector<RowKeys> MultiserverRowKeys;

struct StaticOpLog {
  MultiserverRowKeys oplog_rowkeys;
  OpLogEntries oplog_entries;

  void create_oplog_entry(uint clock_idx) {
    CHECK_LT(clock_idx, oplog_entries.size());
    OpLogEntry& oplog_entry = oplog_entries[clock_idx];
    oplog_entry.resize(oplog_rowkeys.size());
    for (uint server_id = 0; server_id < oplog_rowkeys.size(); server_id++) {
      oplog_entry[server_id].row_updates.
          resize(oplog_rowkeys[server_id].size());
      oplog_entry[server_id].flags.
          resize(oplog_rowkeys[server_id].size());
    }
  }

  void free_oplog_entry(uint clock_idx) {
    if (clock_idx >= oplog_entries.size()) {
      CHECK_EQ(oplog_entries.size(), 0);
      return;
    }
    OpLogEntry& oplog_entry_to_remove = oplog_entries[clock_idx];
    if (oplog_entry_to_remove.size()) {
      /* Use the "swap trick" to force the vector free the memory */
      OpLogEntry empty_oplog_entry;
      oplog_entry_to_remove.swap(empty_oplog_entry);
    }
  }
};

typedef hopscotch_map<RowKey, uint> RowKey2Index;
typedef RowKey2Index ThreadCacheIndex;

struct StaticCacheMetadata {
  uint cache_idx;
  int server_id;
  uint oplog_idx;   /* oplog index */
};
typedef hopscotch_map<RowKey, StaticCacheMetadata> StaticCacheIndex;

struct MutexCvar {
  boost::mutex mutex;
  boost::condition_variable cvar;
};
typedef std::vector<boost::shared_ptr<MutexCvar> > MutexCvars;

struct StaticCache {
  StaticCacheIndex static_cache_index;
  // RowKeys data_cache_row_keys;
  StaticDataCache data_cache;
  StaticOpLog oplog;

  /* Mutex and cvar protecting each row */
  MutexCvars row_mutex_cvars;
  /* Mutex and cvar protecting static_cache_index */
  boost::mutex mutex;
  boost::condition_variable cvar;
};

typedef std::vector<RowData> LocalStorage;
typedef hopscotch_map<RowKey, uint> LocalStorageIndex;

struct ThreadCacheRow {
  bool initted;
  RowKey row_key;
  RowData data;
  iter_t data_age;
  /* We will refresh the thread cache every clock
   * (it's optimization instead of a necessity) */
  iter_t last_refresh;
  int op_flag;
  RowData update;
  StaticCacheMetadata *static_cache_metadata_ptr;
  void init(const RowKey& row_key) {
    this->row_key = row_key;
    initted = true;
    data_age = UNINITIALIZED_CLOCK;
    op_flag = 0;
    last_refresh = UNINITIALIZED_CLOCK;
    static_cache_metadata_ptr = NULL;
  }
  ThreadCacheRow(): initted(false) {}
};
typedef std::vector<ThreadCacheRow> ThreadCache;


struct OpInfo {
  enum OpType {
    READ,
    WRITE,
    CLOCK,
  } type;
  RowKey row_key;
  iter_t slack;
  OpInfo(OpType type, const RowKey& row_key, iter_t slack)
      : type(type), row_key(row_key), slack(slack) {}
  OpInfo(OpType type, const RowKey& row_key)
      : type(type), row_key(row_key) {}
  OpInfo(OpType type)
      : type(type) {}
};
typedef std::vector<OpInfo> OpSeq;

typedef hopscotch_map<RowKey, uint> AccessCount;
struct ReadWrite {
  bool read;
  bool write;
  ReadWrite() {
    read = false;
    write = false;
  }
};
typedef std::vector<ReadWrite> ClockAccessed;   /* Indexed by clock */
struct MergedViInfoRow {
  uint num_threads_access;
  ClockAccessed clock_accessed;
  MergedViInfoRow() : num_threads_access(0) {}
};
typedef hopscotch_map<RowKey, MergedViInfoRow> MergedViInfo;

struct ThreadData {
  uint thread_id;
  iter_t current_clock;
  Stats thread_stats;

  /* For memory affinity */
  uint numa_node_id;

  /* Thread cache storage */
  ThreadCacheIndex thr_cache_index;
  ThreadCache thr_cache;

  /* Local storage */
  LocalStorageIndex local_storage_index;
  LocalStorage local_storage;
  size_t local_storage_size;

  /* Fields for the virtual iteration */
  /* Operation sequence, collected by the virtual iteration */
  OpSeq opseq;
};

class RouterHandler;
class ClientServerEncode;
class ServerClientDecode;
class WorkPusher;
struct CommunicationChannel {
  /* Communication stack */
  boost::shared_ptr<zmq::context_t> zmq_ctx;
  boost::shared_ptr<RouterHandler> router;
  boost::shared_ptr<ClientServerEncode> encoder;
  boost::shared_ptr<ServerClientDecode> decoder;
  boost::shared_ptr<WorkPusher> work_pusher;
  boost::shared_ptr<boost::thread> bg_worker_thread;
  boost::shared_ptr<boost::thread> server_thread;
  /* Shared Cache */
  StaticCache static_cache;
  VecClock server_started;
  iter_t all_server_started;
  uint numa_node_id;
  BgthreadStats bgthread_stats;
  iter_t update_sending_iter;
  VecClock server_clock;
  iter_t shared_iteration;
  /* For the virtual iteration */
  MergedViInfo merged_vi_info;
  iter_t num_vi_clocks;
  boost::shared_ptr<boost::mutex> mutex;
  boost::shared_ptr<boost::condition_variable> cvar;
  CommunicationChannel& operator=(const CommunicationChannel& cc) {
    /* Uncopiable */
    /* STL vector needs that to be defined, but it shouldn't
     * actually be called. */
    return *this;
  }
  CommunicationChannel(const CommunicationChannel& copy) {
    /* Uncopiable */
    /* STL vector needs that, and will call this function even though
     * we are not copying anything. So we just leave it blank. */
  }
  CommunicationChannel() {
    /* Should be constructed mannually */
  }
};

class ServerClientDecode;
class ClientServerEncode;

/* Singleton */
class ClientLib;
extern ClientLib *client_lib;

class ClientLib {
  enum bgworker_cmd_t {
    WORKER_STARTED_CMD = 1,
    PUSH_UPDATES_CMD = 2,
  };
  struct bgcomm_clock_msg_t {
    iter_t clock;
    double progress;
  };

  Stats proc_stats;

  std::vector<CommunicationChannel> comm_channels;
  uint num_channels;

  uint machine_id;

  /* Thread_management */
  boost::mutex global_clock_mutex;
  boost::condition_variable global_clock_cvar;
  boost::mutex server_stats_mutex;
  boost::condition_variable server_stats_cvar;

  boost::thread_specific_ptr<ThreadData> thread_data;
  uint num_threads;

  /* Clock for this client process */
  VecClock vec_clock;
  iter_t global_clock;
  iter_t fast_clock;
  vector<double> worker_progress;

  /* Server states */
  vector<string> host_list;
  vector<uint> port_list;
  uint tcp_base_port;
  uint num_machines;

  /* Fields for the virtual iteration */
  VecClock virtual_iteration_states;
  boost::mutex virtual_iteration_mutex;
  boost::condition_variable virtual_iteration_cvar;

  /* Log states */
  tbb::tick_count fast_thread_tick;

  /* Config */
  Config config;

 private:
  /* This class allows a singleton object.  Hence the private constructor. */
  ClientLib(uint machine_id, const Config& config);

  /* Helper functions */
  void init_comm_channel(uint channel_id, const Config& config);
  uint get_machine_id(const RowKey& row_key);
  uint get_machine_id(uint64_t hash);
  uint get_channel_id(const RowKey& row_key);
  uint get_channel_id(uint64_t hash);

  /* Thread cache functions */
  void init_thr_cache_row(
      ThreadCacheRow& thr_cache_row, const RowKey& row_key);
  ThreadCacheRow *get_thr_cache(
      ThreadData& thread_data_ref, const RowKey& row_key);
  void flush_thr_oplog_row(
      ThreadCacheRow& thread_cache_row, iter_t clock);
  void flush_thr_oplog(iter_t clock);

  /* Static cache functions */
  /* Return values:
   * 0: succeed
   * otherwise: error code
   *   -1: row not found */
  int read_row_static_cache(
      RowData *buffer, const RowKey& row_key,
      iter_t clock, iter_t required_data_age,
      uint channel_id, iter_t *data_age_ptr, bool timing);
  int update_row_static_cache(
      const RowKey& row_key, const RowData *update,
      uint channel_id, bool timing);
  int recv_row_static_cache(
      uint channel_id, const RowKey& row_key,
      iter_t data_age, iter_t self_clock,
      const RowData& data, bool timing);
  void push_updates_static_cache(uint channel_id, iter_t clock);
  int find_row_static_cache(
      const RowKey& row_key, uint channel_id,
      bool unique_request = true, bool non_blocking = false,
      bool force_refresh = false);
  int find_row_cbk_static_cache(
      const RowKey& row_key, uint32_t server_id, uint channel_id);
  void create_shared_oplog_entries(iter_t clock);

  /* Functions for virtual iteration */
  void virtual_op(const OpInfo& opinfo);
  void vi_thread_summarize();
  void vi_process_decisions();
  void vi_thread_decisions();
  void vi_process_channel_decisions(uint channel_id);

 public:
  static void CreateInstance(uint machine_id, const Config& config) {
    if (client_lib == NULL) {
      client_lib = new ClientLib(machine_id, config);
    }
  }

  /* Functions called by the application code */
  std::string json_stats();
  void thread_start();
  void thread_stop();
  void shutdown();
  void iterate();
  void update_row(const RowKey& row_key, const RowData *update);
  void read_row(RowData *buffer, const RowKey& row_key, iter_t slack);
  RowData *local_access(const RowKey& row_key);
  void iterate(double progress);
  void virtual_read(const RowKey& row_key, iter_t slack);
  void virtual_write(const RowKey& row_key);
  void virtual_local_access(const RowKey& row_key);
  void virtual_clock();
  void finish_virtual_iteration();

  /* Functions used by the pusher thread */
  void signal_bg_work__worker_started(uint channel_id);
  void perform_bg_work__worker_started(
      uint channel_id, vector<ZmqPortableBytes>& args);
  void worker_started(uint channel_id);
  void signal_bg_work__push_updates(
      uint channel_id, iter_t clock, double progress);
  void perform_bg_work__push_updates(
      uint channel_id, vector<ZmqPortableBytes>& msgs);
  void push_updates(
      uint channel_id, iter_t clock, double progress);

  /* Functions used by the puller thread */
  void find_row_cbk(const RowKey& row_key, uint server_id);
  void recv_row_batch(
      uint channel_id, iter_t data_age, iter_t self_clock,
      RowKey *row_keys, RowData *row_data, uint batch_size);
  void server_started(uint channel_id, uint server_id);
  void server_iterate(uint channel_id, uint tablet_id, iter_t iter);
  void get_stats_cbk(const string& server_stats);
};

#endif  // defined __lazytable_impl_hpp__
