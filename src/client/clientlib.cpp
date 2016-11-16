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

#include <boost/lexical_cast.hpp>

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
using std::make_pair;
using boost::format;
using boost::make_shared;
using boost::shared_ptr;
using boost::mutex;
using boost::condition_variable;

ClientLib *client_lib = NULL;

ClientLib::ClientLib(
    uint machine_id, const Config& config)
        : machine_id(machine_id), config(config) {
  /* Init glog */
  google::InstallFailureSignalHandler();

  /* Takes in configs */
  if (config.update_func) {
    CHECK(!config.read_my_writes);
  }
  host_list = config.host_list;
  num_channels = config.num_channels;
  tcp_base_port = config.tcp_base_port;
  num_machines = config.host_list.size();

  /* Init the tablet server and communication modules */
  comm_channels.resize(num_channels);
  for (uint channel_id = 0; channel_id < num_channels; channel_id++) {
    init_comm_channel(channel_id, config);
  }

  /* Init fields */
  vec_clock.resize(config.num_threads);
  virtual_iteration_states.resize(config.num_threads);
  for (uint i = 0; i < vec_clock.size(); i++) {
    vec_clock[i] = UNINITIALIZED_CLOCK;
  }
  global_clock = UNINITIALIZED_CLOCK;
  fast_clock = UNINITIALIZED_CLOCK;
  num_threads = 0;
}

void ClientLib::init_comm_channel(
        uint channel_id,
        const Config& config) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  comm_channel.numa_node_id = 0;
  if (config.affinity) {
    comm_channel.numa_node_id =
      get_numa_node_id(channel_id, config.num_channels, config.num_zones);
  }
  comm_channel.mutex = make_shared<boost::mutex>();
  comm_channel.cvar = make_shared<boost::condition_variable>();

  comm_channel.zmq_ctx = make_shared<zmq::context_t>(1);

  ServerThreadEntry server_executer(
      channel_id, num_channels, machine_id, num_machines,
      comm_channel.zmq_ctx, config);
  comm_channel.server_thread = make_shared<boost::thread>(server_executer);

  string client_name = (format("client-%i") % machine_id).str();
  vector<string> bind_list;   /* Empty bind_list vector */
  vector<string> connect_list;
  CHECK(host_list.size());
  for (uint i = 0; i < host_list.size(); i ++) {
    uint port =
      channel_id + ((port_list.size() != 0) ? port_list[i] : tcp_base_port);
    string connect_endpoint =
        "tcp://" + host_list[i] + ":" + boost::lexical_cast<std::string>(port);
    connect_list.push_back(connect_endpoint);
  }
  comm_channel.router = make_shared<RouterHandler>(
      channel_id, comm_channel.zmq_ctx, connect_list, bind_list,
      client_name, comm_channel.numa_node_id, config);
  comm_channel.encoder = make_shared<ClientServerEncode>(
      comm_channel.router, host_list.size(), machine_id, config);

  comm_channel.decoder = make_shared<ServerClientDecode>(
      channel_id, comm_channel.zmq_ctx,
      this, true /* Work in background */,
      comm_channel.numa_node_id, config);
  comm_channel.router->start_handler_thread(
      comm_channel.decoder->get_recv_callback());

  /* Start background worker thread */
  string endpoint = "inproc://bg-worker";
  shared_ptr<WorkPuller> work_puller =
      make_shared<WorkPuller>(comm_channel.zmq_ctx, endpoint);
  BackgroundWorker bg_worker(
      channel_id, work_puller, comm_channel.numa_node_id, config);
  BackgroundWorker::WorkerCallback worker_started_callback =
      bind(&ClientLib::perform_bg_work__worker_started, this, channel_id, _1);
  bg_worker.add_callback(WORKER_STARTED_CMD, worker_started_callback);
  BackgroundWorker::WorkerCallback push_updates_callback =
      bind(&ClientLib::perform_bg_work__push_updates, this, channel_id, _1);
  bg_worker.add_callback(PUSH_UPDATES_CMD, push_updates_callback);
  comm_channel.bg_worker_thread = make_shared<boost::thread>(bg_worker);

  /* Init work pusher */
  comm_channel.work_pusher = make_shared<WorkPusher>(
                comm_channel.zmq_ctx, endpoint);

  /* Init other fields */
  comm_channel.update_sending_iter = 0;
  comm_channel.shared_iteration = 0;
  comm_channel.server_started.resize(num_machines);
  for (uint i = 0; i < num_machines; i++) {
    comm_channel.server_started[i] = 0;
  }
  comm_channel.all_server_started = 0;
  comm_channel.server_clock.resize(num_machines);
  for (uint server_id = 0; server_id < num_machines; server_id++) {
    comm_channel.server_clock[server_id] = UNINITIALIZED_CLOCK;
  }
}

void ClientLib::thread_start() {
  ScopedLock global_clock_lock(global_clock_mutex);

  /* Set affinities */
  uint thread_id = num_threads;
  uint numa_node_id = 0;
  if (config.affinity) {
    numa_node_id =
      get_numa_node_id(thread_id, config.num_threads, config.num_zones);
    set_cpu_affinity(numa_node_id, config.num_cores, config.num_zones);
    set_mem_affinity(numa_node_id);
  }

  thread_data.reset(new ThreadData);
  ThreadData& thread_data_ref = *thread_data;

  /* Set up thread number */
  thread_data_ref.thread_id = thread_id;
  thread_data_ref.numa_node_id = numa_node_id;
  num_threads++;
  proc_stats.num_threads++;

  /* Set up clocks */
  thread_data_ref.current_clock = INITIAL_CLOCK;
  CHECK_EQ(global_clock, UNINITIALIZED_CLOCK);
  vec_clock[thread_data_ref.thread_id] = INITIAL_CLOCK;
  virtual_iteration_states[thread_data_ref.thread_id] = 0;
  if (clock_min(vec_clock) == INITIAL_CLOCK) {
    /* All threads started in this client.
     * Signal the background worker to send a worker started message
     * to all servers */
    for (uint channel_id = 0; channel_id < comm_channels.size(); channel_id++) {
      signal_bg_work__worker_started(channel_id);
    }
  }

  /* Init other fields */
  thread_data_ref.local_storage_size = 0;
}

void ClientLib::shutdown() {
  // for (uint i = 0; i < num_channels; i++) {
    // zmq_term(*(comm_channels[i].zmq_ctx));
  // }
  /* TODO: join threads */
  for (uint i = 0; i < num_channels; i++) {
    CommunicationChannel& comm_channel = comm_channels[i];
    /* Shut down background worker thread */
    comm_channel.work_pusher->push_work(BackgroundWorker::STOP_CMD);
    (*comm_channel.bg_worker_thread).join();

    /* Shut down router thread */
    comm_channel.router->stop_handler_thread();

    /* Shut down decoder thread */
    comm_channel.decoder->stop_decoder();
  }
}

void ClientLib::thread_stop() {
  ScopedLock global_clock_lock(global_clock_mutex);
  num_threads--;
  thread_data.release();
}

uint ClientLib::get_machine_id(const RowKey& row_key) {
  return get_machine_id(row_key.get_hash());
}

uint ClientLib::get_machine_id(uint64_t hash) {
  return hash % (num_machines * num_channels) / num_channels;
}

uint ClientLib::get_channel_id(const RowKey& row_key) {
  return get_channel_id(row_key.get_hash());
}

uint ClientLib::get_channel_id(uint64_t hash) {
  return hash % num_channels;
}

string ClientLib::json_stats() {
  uint channel_id = 0;
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  proc_stats.router_stats = comm_channel.router->get_stats();
  /* TODO separate bgthread_stats */
  proc_stats.bgthread_stats = comm_channel.bgthread_stats.to_json();

  /* Get server stat */
  ScopedLock server_stats_lock(server_stats_mutex);
  bool call_server = true;
  proc_stats.server_stats_refreshed = false;
  while (!proc_stats.server_stats_refreshed) {
    if (call_server) {
      call_server = false;
      server_stats_lock.unlock();
          /* Release the lock while sending messages */
      comm_channel.encoder->get_stats(machine_id);
      server_stats_lock.lock();
    } else {
      server_stats_cvar.wait(server_stats_lock);
          /* Wait is notified by get_stats_cbk() */
    }
  }

  string json = proc_stats.to_json();
  return json;
}

void ClientLib::iterate(double progress) {
  tbb::tick_count iterate_start = tbb::tick_count::now();

  ThreadData& thread_data_ref = *thread_data;

  CHECK_GE(thread_data_ref.current_clock, 0);
  iter_t clock = thread_data_ref.current_clock;
  /* Increment thread-local clock */
  thread_data_ref.current_clock++;
  iter_t new_clock = thread_data_ref.current_clock;

  // cout << "Client " << machine_id
      // << " thread " << thread_data_ref.thread_id
      // << " finished clock " << clock << endl;

  /* First flush thread-local logs to shared log */
  flush_thr_oplog(clock);

  thread_data_ref.thread_stats.iter_flush_log_time +=
    (tbb::tick_count::now() - iterate_start).seconds();

  {
    ScopedLock global_clock_lock(global_clock_mutex);

    /* Update worker progress */
    uint progress_idx = static_cast<uint>(clock);
    if (progress_idx >= worker_progress.size()) {
      worker_progress.push_back(0.0);
    }
    CHECK_LT(progress_idx, worker_progress.size());
    worker_progress[progress_idx] += progress;

    /* Update process stats */
    proc_stats += thread_data_ref.thread_stats;
    thread_data_ref.thread_stats = Stats();

    if (clock_max(vec_clock) < new_clock) {
      /* First thread to finish this clock */
      create_shared_oplog_entries(new_clock);
      fast_clock = new_clock;
      fast_thread_tick = tbb::tick_count::now();;
    }

    /* Increment process vector clock */
    CHECK_GE(vec_clock[thread_data_ref.thread_id], 0);
    CHECK_EQ(vec_clock[thread_data_ref.thread_id] + 1, new_clock);
    vec_clock[thread_data_ref.thread_id] = new_clock;

    if (clock_min(vec_clock) == new_clock) {
      /* Last thread to finish this clock */
      /* Global_clock = t means that all clients have finished clock t */
      CHECK_EQ(clock, new_clock - 1);
      global_clock = clock;

      double var_time = (tbb::tick_count::now() - fast_thread_tick).seconds();
      proc_stats.iter_var_time += var_time;
      // cout << "Client: " << machine_id
           // << " Slow thread: " << thread_data_ref.thread_id
           // << " var_time: " << var_time
           // << endl;

      iter_t signalled_clock = clock;
      uint progress_idx = static_cast<uint>(clock);
      double progress = worker_progress[progress_idx];
      /* Let the background communication thread send updates to the servers */
      for (uint channel_id = 0; channel_id < comm_channels.size(); channel_id++) {
        /* Signal the background thread to send updates */
        signal_bg_work__push_updates(
            channel_id, signalled_clock, progress);
      }
    }
  }   /* Unlock global_clock_lock */

  thread_data_ref.thread_stats.iterate_time +=
        (tbb::tick_count::now() - iterate_start).seconds();
}

void ClientLib::read_row(
    RowData *buffer, const RowKey& row_key, iter_t slack) {
  ThreadData& thread_data_ref = *thread_data;
  iter_t clock = thread_data_ref.current_clock;
  iter_t required_data_age = clock - 1 - slack;
  iter_t data_age;
  int timing = READ_TIMING_FREQ != 0 &&
        thread_data->thread_stats.total_read % READ_TIMING_FREQ == 0;
  tbb::tick_count read_row_start;
  thread_data_ref.thread_stats.total_read++;
  if (timing) {
    read_row_start = tbb::tick_count::now();
  }

  ThreadCacheRow *thr_cache_row_ptr =
      get_thr_cache(thread_data_ref, row_key);

  /* Check thread cache */
  if (thr_cache_row_ptr != NULL) {
    ThreadCacheRow& thr_cache_row = *thr_cache_row_ptr;
    if (thr_cache_row.last_refresh < clock) {
      /* We will refresh the thread cache every iteration (it's an
       * optimization instead of a necessity). So now we don't read
       * from thread cache. */
      thr_cache_row.last_refresh = clock;
    } else {
      /* Read from thread cache */
      CHECK_GE(thr_cache_row.data_age, required_data_age);
      thread_data_ref.thread_stats.num_thread_hit++;
      if (buffer != NULL) {
        *buffer = thr_cache_row.data;
      }
      /* Read row ends */
      if (timing) {
        thread_data_ref.thread_stats.read_time +=
          (tbb::tick_count::now() - read_row_start).seconds();
      }
      return;
    }
  }

  uint channel_id = get_channel_id(row_key);
  /* Read from static cache */
  int ret = read_row_static_cache(
      buffer, row_key, clock, required_data_age,
      channel_id, &data_age, timing);
  CHECK_EQ(ret, 0);

  if (buffer != NULL && thr_cache_row_ptr != NULL) {
    ThreadCacheRow& thr_cache_row = *thr_cache_row_ptr;
    if (config.read_my_writes) {
      /* Apply thread-local updates */
      if (thr_cache_row.op_flag) {
        *buffer += thr_cache_row.update;
      }
    }
    /* Update thread cache */
    thr_cache_row.data_age = data_age;
    thr_cache_row.data = *buffer;
  }

  if (timing) {
    thread_data_ref.thread_stats.read_time +=
      (tbb::tick_count::now() - read_row_start).seconds();
  }
}

void ClientLib::update_row(
      const RowKey& row_key, const RowData *update) {
  /* TLS is slow to locate, we make a reference of it */
  ThreadData& thread_data_ref = *thread_data;
  int timing =
      INCREMENT_TIMING_FREQ != 0 &&
        thread_data_ref.thread_stats.total_increment %
            INCREMENT_TIMING_FREQ == 0;
  thread_data_ref.thread_stats.total_increment++;
  tbb::tick_count increment_start;
  if (timing) {
    increment_start = tbb::tick_count::now();
  }

  /* Look at thread cache */
  ThreadCacheRow *thr_cache_row_ptr =
      get_thr_cache(thread_data_ref, row_key);

  if (thr_cache_row_ptr != NULL) {
    ThreadCacheRow& thr_cache_row = *thr_cache_row_ptr;
    thr_cache_row.op_flag = 1;
    if (config.read_my_writes) {
      thr_cache_row.data += *update;
    }
    thr_cache_row.update += *update;

    thread_data_ref.thread_stats.total_apply_thr_cache++;
    if (timing) {
      thread_data_ref.thread_stats.increment_time +=
          (tbb::tick_count::now() - increment_start).seconds();
    }
    return;
  }

  uint channel_id = get_channel_id(row_key);
  /* Look at static cache */
  int ret = update_row_static_cache(row_key, update, channel_id, timing);
  CHECK_EQ(ret, 0);
  if (timing) {
    thread_data_ref.thread_stats.increment_time +=
      (tbb::tick_count::now() - increment_start).seconds();
  }
}

RowData *ClientLib::local_access(const RowKey& row_key) {
  ThreadData& thread_data_ref = *thread_data;
  LocalStorage& local_storage = thread_data_ref.local_storage;
  LocalStorageIndex& local_storage_index = thread_data_ref.local_storage_index;
  LocalStorageIndex::iterator index_it = local_storage_index.find(row_key);
  CHECK(index_it != local_storage_index.end());
  uint row_idx = index_it->second;
  CHECK_LT(row_idx, local_storage.size());
  RowData *row_data = &local_storage[row_idx];
  return row_data;
}
