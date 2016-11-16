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

#include <utility>
#include <string>
#include <vector>

#include "tablet-server.hpp"
#include "server-encoder-decoder.hpp"

using std::string;
using std::cerr;
using std::cout;
using std::endl;
using std::vector;
using std::pair;
using std::make_pair;
using boost::format;
using boost::lexical_cast;
using boost::shared_ptr;
using boost::make_shared;


TabletStorage::TabletStorage(
      uint channel_id, uint num_channels, uint machine_id, uint num_machines,
      shared_ptr<ServerClientEncode> comm,
      const Config& config) :
    channel_id(channel_id), num_channels(num_channels),
    machine_id(machine_id), num_machines(num_machines),
    client_count(num_machines),
    communicator(comm),
    all_client_subscribed_reads(num_machines),
    config(config) {
  global_clock = UNINITIALIZED_CLOCK;
  row_count = 0;

  /* Initialize server fields */
  worker_started_states.resize(num_machines);
  for (uint i = 0; i < num_machines; i++) {
    worker_started_states[i] = 0;
  }
  all_workers_started = 0;
  vec_clock.resize(num_machines);
  for (uint i = 0; i < num_machines; i++) {
    vec_clock[i] = UNINITIALIZED_CLOCK;
  }
}

uint TabletStorage::get_row_idx(const RowKey& row_key) {
  uint row_idx;
  Row2Index::iterator row2idx_it = row2idx.find(row_key);
  if (row2idx_it == row2idx.end()) {
    row_idx = row_count;
    row2idx[row_key] = row_idx;
    row_count++;
    TableStorage& table_storage =
        model_storage.storage;
    resize_storage<RowStorage>(table_storage, row_count);
    RowStorage& row_storage = table_storage[row_idx];
    row_storage.init(row_key);

    /* Init stores according to the update function */
    switch (config.update_func) {
    case Config::UPDATE_FUNC_ADD: {
      /* Blank */
      break;
    }
    case Config::UPDATE_FUNC_STEPSIZE: {
      break;
    }
    case Config::UPDATE_FUNC_ADAGRAD: {
      /* AdaGrad */
      TableStorage& accum_gradients_store =
          model_storage.accum_gradients_store;
      resize_storage<RowStorage>(accum_gradients_store, row_count);
      RowStorage& accum_gradients_row = accum_gradients_store[row_idx];
      accum_gradients_row.init(row_key);
      break;
    }
    case Config::UPDATE_FUNC_ADAREVISION: {
      /* AdaRevision */
      TableStorage& accum_gradients_store =
          model_storage.accum_gradients_store;
      resize_storage<RowStorage>(accum_gradients_store, row_count);
      RowStorage& accum_gradients_row = accum_gradients_store[row_idx];
      accum_gradients_row.init(row_key);
      TableStorage& old_accum_gradients_store =
          model_storage.old_accum_gradients_store;
      resize_storage<RowStorage>(old_accum_gradients_store, row_count);
      RowStorage& old_accum_gradients_row = old_accum_gradients_store[row_idx];
      old_accum_gradients_row.init(row_key);
      TableStorage& z_store = model_storage.z_store;
      resize_storage<RowStorage>(z_store, row_count);
      RowStorage& z_row = z_store[row_idx];
      z_row.init(row_key);
      TableStorage& z_max_store = model_storage.z_max_store;
      resize_storage<RowStorage>(z_max_store, row_count);
      RowStorage& z_max_row = z_max_store[row_idx];
      z_max_row.init(row_key);
      break;
    }
    default: {
      CHECK(0) << " known update func: " << config.update_func;
    }
    }
  } else {
    row_idx = row2idx_it->second;
  }
  return row_idx;
}

void TabletStorage::subscribe_rows(
      uint client_id, RowKeys& row_keys) {
  SubscribedReads& subscribed_reads = all_client_subscribed_reads[client_id];
  CHECK_EQ(subscribed_reads.size(), 0);
  subscribed_reads.resize(row_keys.size()); 
  subscribed_reads.row_keys = row_keys;
}

void TabletStorage::update_row_batch(
      uint client_id, iter_t clock,
      uint batch_size, RowKey *row_keys, RowData *row_updates) {
  server_stats.num_update += batch_size;
  if (client_id == machine_id) {
    server_stats.num_local_update += batch_size;
  }

  iter_t cur_clock = vec_clock[client_id];
  if (cur_clock != UNINITIALIZED_CLOCK) {
    CHECK_EQ(clock, cur_clock + 1);
  }

  for (uint i = 0; i < batch_size; i++) {
    RowKey& row_key = row_keys[i];
    uint row_idx = get_row_idx(row_key);
    const RowData& update_row = row_updates[i];
    if (cur_clock == UNINITIALIZED_CLOCK) {
      /* We assume we use the first clock to initialize the param data */
      apply_updates_blank(update_row, row_idx, config.tunable);
      continue;
    }
    /* Apply updates according to the update function */
    switch (config.update_func) {
    case Config::UPDATE_FUNC_ADD: {
      /* Blank */
      apply_updates_blank(update_row, row_idx, config.tunable);
      break;
    }
    case Config::UPDATE_FUNC_STEPSIZE: {
      apply_updates_stepsize(update_row, row_idx, config.tunable);
      break;
    }
    case Config::UPDATE_FUNC_ADAGRAD: {
      /* AdaGrad */
      apply_updates_adagrad(update_row, row_idx, config.tunable);
      break;
    }
    case Config::UPDATE_FUNC_ADAREVISION: {
      /* AdaRevision */
      apply_updates_adarevision(update_row, row_idx, config.tunable);
      break;
    }
    default: {
      CHECK(0) << " known update func: " << config.update_func;
    }
    }
  }
}

void TabletStorage::apply_updates_blank(
    const RowData& update_row, uint row_idx,
    const Tunable& tunable) {
  const val_t *update = update_row.data;
  val_t *master_data =
      model_storage.storage[row_idx].data.data;
  for (uint i = 0; i < ROW_DATA_SIZE; i++) {
    master_data[i] += update[i];
  }
}

void TabletStorage::apply_updates_stepsize(
    const RowData& update_row, uint row_idx,
    const Tunable& tunable) {
  val_t learning_rate = tunable.learning_rate;
  const val_t *update = update_row.data;
  val_t *master_data =
      model_storage.storage[row_idx].data.data;

  for (uint i = 0; i < ROW_DATA_SIZE; i++) {
    master_data[i] += learning_rate * update[i];
  }
}

void TabletStorage::apply_updates_adagrad(
    const RowData& update_row, uint row_idx,
    const Tunable& tunable) {
  val_t learning_rate = tunable.learning_rate;
  const val_t *gradient = update_row.data;
  val_t *master_data =
      model_storage.storage[row_idx].data.data;
  val_t *accum_gradients =
      model_storage.accum_gradients_store[row_idx].data.data;
  val_t delta = 1;

  for (uint i = 0; i < ROW_DATA_SIZE; i++) {
    val_t update_square = gradient[i] * gradient[i];
    accum_gradients[i] += update_square;
    val_t accum_gradients_sqrt = sqrt(accum_gradients[i] + delta);
    val_t adjusted_update =
        learning_rate * gradient[i] / accum_gradients_sqrt;
    master_data[i] += adjusted_update;
  }
}

void TabletStorage::apply_updates_adarevision(
    const RowData& update_row, uint row_idx,
    const Tunable& tunable) {
  val_t learning_rate = tunable.learning_rate;
  const val_t *gradient = update_row.data;
  val_t *master_data =
      model_storage.storage[row_idx].data.data;
  val_t *accum_gradients =
      model_storage.accum_gradients_store[row_idx].data.data;
  val_t *old_accum_gradients =
      model_storage.old_accum_gradients_store[row_idx].data.data;
  val_t *z =
      model_storage.z_store[row_idx].data.data;
  val_t *z_max =
      model_storage.z_max_store[row_idx].data.data;
  val_t delta = 1;

  for (uint i = 0; i < ROW_DATA_SIZE; i++) {
    val_t g_bck = accum_gradients[i] - old_accum_gradients[i];
    val_t eta_old = learning_rate / sqrt(z_max[i] + delta);
    z[i] += gradient[i] * gradient[i] + 2 * gradient[i] * g_bck;
    z_max[i] = std::max(z[i], z_max[i]);
    val_t eta = learning_rate / sqrt(z_max[i] + delta);
    val_t adjusted_update = eta * gradient[i] + (eta - eta_old) * g_bck;
    master_data[i] += adjusted_update;
    accum_gradients[i] += gradient[i];
  }
}

void TabletStorage::process_all_client_subscribed_reads(iter_t clock) {
  /* Not starting from the same client */
  uint client_to_start = clock % client_count;
  for (uint i = 0; i < client_count; i++) {
    uint client_id = (client_to_start + i) % client_count;
    SubscribedReads& subscribed_reads =
        all_client_subscribed_reads[client_id];
    process_subscribed_reads(client_id, clock, subscribed_reads);
  }
}

void TabletStorage::process_subscribed_reads(
    uint client_id, iter_t data_age, SubscribedReads& subscribed_reads) {
  if (subscribed_reads.size()) {
    TableStorage& table_storage = model_storage.storage;
    for (uint i = 0; i < subscribed_reads.size(); i++) {
      uint row_idx = get_row_idx(subscribed_reads.row_keys[i]);
      CHECK_LT(row_idx, table_storage.size());
      RowStorage& row_storage = table_storage[row_idx];
      subscribed_reads.row_data_batch[i] = row_storage.data;
    }
    iter_t self_clock = vec_clock[client_id];
    communicator->read_row_batch(
        client_id, data_age, self_clock,
        subscribed_reads.size(), subscribed_reads.row_keys.data(),
        subscribed_reads.row_data_batch.data());
    // cout << "Server send data of clock " << data_age << endl;
  }
}

void TabletStorage::clock(uint client_id, iter_t clock) {
  int timing = true;
  tbb::tick_count clock_ad_start;

  if (timing) {
    clock_ad_start = tbb::tick_count::now();
  }

  // if (machine_id == 0 && channel_id == 0) {
    // cout << "Client " << client_id << " finished clock " << clock << endl;
  // }

  if (clock_max(vec_clock) < clock) {
    first_come_time = tbb::tick_count::now();
  }

  if (vec_clock[client_id] != UNINITIALIZED_CLOCK) {
    CHECK_EQ(clock, vec_clock[client_id] + 1);
  }
  vec_clock[client_id] = clock;
  iter_t new_global_clock = clock_min(vec_clock);
  if (new_global_clock != global_clock) {
    if (global_clock != UNINITIALIZED_CLOCK) {
      CHECK_EQ(new_global_clock, global_clock + 1);
    }
    global_clock = new_global_clock;
    double iter_var_time = (tbb::tick_count::now() - first_come_time).seconds();
    server_stats.iter_var_time += iter_var_time;

    if (machine_id == 0 && channel_id == 0) {
      double client_progress = 0.0;
      uint global_clock_uint = static_cast<uint>(global_clock);
      if (global_clock_uint < client_progresses.size()) {
        client_progress = client_progresses[global_clock_uint];
      }
      cout << "Finished clock: " << global_clock
           // << " server: " << machine_id << " channel: " << channel_id
           << " loss: " << client_progress
           << " time: " << (tbb::tick_count::now() - start_time).seconds()
           << endl;
    }

    if (config.update_func == Config::UPDATE_FUNC_ADAREVISION) {
      /* Save the old accumulated gradient */
      TableStorage& accum_gradients_store =
          model_storage.accum_gradients_store;
      TableStorage& old_accum_gradients_store =
          model_storage.old_accum_gradients_store;
      CHECK_EQ(old_accum_gradients_store.size(), accum_gradients_store.size());
      memcpy(old_accum_gradients_store.data(), accum_gradients_store.data(),
          accum_gradients_store.size() * sizeof(RowStorage));
    }

    /* Send subscribed reads */
    process_all_client_subscribed_reads(global_clock);

    /* Notify clients of new iteration */
    communicator->clock(machine_id, global_clock);
  }

  if (timing) {
    server_stats.iterate_time +=
      (tbb::tick_count::now() - clock_ad_start).seconds();
  }
}

void TabletStorage::get_stats(
    uint client_id, shared_ptr<MetadataServer> metadata_server) {
  server_stats.num_rows = row_count;

  std::stringstream combined_server_stats;
  combined_server_stats << "{"
         << "\"storage\": " << server_stats.to_json() << ", "
         << "\"metadata\": " << metadata_server->get_stats() << ", "
         << "\"router\": " << communicator->get_router_stats()
         << " } ";
  communicator->get_stats(client_id, combined_server_stats.str());
}

void TabletStorage::worker_started(uint client_id) {
  CHECK_LT(client_id, worker_started_states.size());
  worker_started_states[client_id] = 1;
  all_workers_started = clock_min(worker_started_states);
  if (all_workers_started) {
    start_time = tbb::tick_count::now();
    communicator->server_started(machine_id);
  }
}

void TabletStorage::report_progress(
    uint client_id, int clock, double progress) {
  CHECK_GE(clock, 0);
  uint clock_uint = static_cast<uint>(clock);
  while (clock_uint >= client_progresses.size()) {
    client_progresses.push_back(0.0);
  }
  CHECK_LT(clock_uint, client_progresses.size());
  client_progresses[clock_uint] += progress;
}
