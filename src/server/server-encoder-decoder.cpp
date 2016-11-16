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

#include "server-encoder-decoder.hpp"
#include "common/portable-bytes.hpp"

using std::string;
using std::cerr;
using std::cout;
using std::endl;
using std::vector;
using std::pair;
using boost::shared_ptr;

ClientServerDecode::ClientServerDecode(
    shared_ptr<TabletStorage> storage,
    shared_ptr<MetadataServer> metadata_server) :
      storage(storage), metadata_server(metadata_server) {}

void ClientServerDecode::find_row(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(cs_find_row_msg_t));

  cs_find_row_msg_t *cs_find_row_msg =
      reinterpret_cast<cs_find_row_msg_t *>(args[0].data());
  uint client_id = cs_find_row_msg->client_id;
  RowKey& row_key = cs_find_row_msg->row_key;

  metadata_server->find_row(client_id, row_key);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::iterate(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(cs_iterate_msg_t));

  cs_iterate_msg_t *cs_iterate_msg =
      reinterpret_cast<cs_iterate_msg_t *>(args[0].data());
  uint client_id = cs_iterate_msg->client_id;
  iter_t clock = cs_iterate_msg->clock;

  storage->clock(client_id, clock);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::clock_with_updates_batch(
    vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 3);
  CHECK_EQ(args[0].size(), sizeof(cs_clock_with_updates_batch_msg_t));

  cs_clock_with_updates_batch_msg_t *cs_clock_with_updates_batch_msg =
      reinterpret_cast<cs_clock_with_updates_batch_msg_t *>(args[0].data());
  uint client_id = cs_clock_with_updates_batch_msg->client_id;
  iter_t clock = cs_clock_with_updates_batch_msg->clock;

  RowKey *row_keys =
      reinterpret_cast<RowKey *>(args[1].data());
  uint batch_size = args[1].size() / sizeof(RowKey);
  RowData *row_updates =
      reinterpret_cast<RowData *>(args[2].data());
  CHECK_EQ(batch_size, args[2].size() / sizeof(RowData));

  tbb::tick_count update_start = tbb::tick_count::now();
  storage->update_row_batch(
      client_id, clock, batch_size, row_keys, row_updates);
  storage->server_stats.update_time +=
      (tbb::tick_count::now() - update_start).seconds();
  storage->clock(client_id, clock);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::subscribe_rows(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 2);
  CHECK_EQ(args[0].size(),sizeof(cs_subscribe_rows_msg_t));

  cs_subscribe_rows_msg_t *cs_subscribe_rows_msg =
      reinterpret_cast<cs_subscribe_rows_msg_t *>(args[0].data());
  uint client_id = cs_subscribe_rows_msg->client_id;

  RowKeys row_keys;
  args[1].unpack_vector<RowKey>(row_keys);
  storage->subscribe_rows(client_id, row_keys);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::report_access_info(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 2);
  CHECK_EQ(args[0].size(), sizeof(cs_report_access_info_msg_t));

  cs_report_access_info_msg_t *cs_report_access_info_msg =
      reinterpret_cast<cs_report_access_info_msg_t *>(args[0].data());
  uint client_id = cs_report_access_info_msg->client_id;

  vector<RowAccessInfo> access_info;
  args[1].unpack_vector<RowAccessInfo>(access_info);

  metadata_server->report_access_info(client_id, access_info);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::get_stats(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(cs_get_stats_msg_t));

  cs_get_stats_msg_t *cs_get_stats_msg =
      reinterpret_cast<cs_get_stats_msg_t *>(args[0].data());
  uint client_id = cs_get_stats_msg->client_id;

  storage->get_stats(client_id, metadata_server);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::worker_started(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(cs_worker_started_msg_t));
  cs_worker_started_msg_t *cs_worker_started_msg =
      reinterpret_cast<cs_worker_started_msg_t *>(args[0].data());
  uint client_id = cs_worker_started_msg->client_id;

  storage->worker_started(client_id);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::report_progress(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(cs_report_progress_msg_t));
  cs_report_progress_msg_t *cs_report_progress_msg =
      reinterpret_cast<cs_report_progress_msg_t *>(args[0].data());
  uint client_id = cs_report_progress_msg->client_id;
  int clock = cs_report_progress_msg->clock;
  val_t progress = cs_report_progress_msg->progress;

  storage->report_progress(client_id, clock, progress);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ClientServerDecode::decode_msg(vector<ZmqPortableBytes>& msgs) {
  CHECK_GE(msgs.size(), 1);
  CHECK_GE(msgs[0].size(), sizeof(command_t));

  command_t cmd;
  msgs[0].unpack<command_t>(cmd);
  switch (cmd) {
  case FIND_ROW:
    find_row(msgs);
    break;
  case CLOCK_WITH_UPDATES_BATCH:
    clock_with_updates_batch(msgs);
    break;
  case SUBSCRIBE_ROWS:
    subscribe_rows(msgs);
    break;
  case ITERATE:
    iterate(msgs);
    break;
  case REPORT_ACCESS_INFO:
    report_access_info(msgs);
    break;
  case GET_STATS:
    get_stats(msgs);
    break;
  case WORKER_STARTED:
    worker_started(msgs);
    break;
  case REPORT_PROGRESS:
    report_progress(msgs);
    break;
  default:
    cerr << "Server received unknown command: " << static_cast<int>(cmd)
         << " size: " << msgs[0].size()
         << endl;
    CHECK(0);
  }
}

void ClientServerDecode::router_callback(
    const string& src, vector<ZmqPortableBytes>& msgs) {
  decode_msg(msgs);
}

RouterHandler::RecvCallback ClientServerDecode::get_recv_callback() {
  return bind(&ClientServerDecode::router_callback, this, _1, _2);
}


void ServerClientEncode::clock(uint32_t server_id, iter_t clock) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(sc_iterate_msg_t));
  sc_iterate_msg_t *sc_iterate_msg =
      reinterpret_cast<sc_iterate_msg_t *>(msgs[0].data());
  sc_iterate_msg->cmd = ITERATE;
  sc_iterate_msg->server_id = server_id;
  sc_iterate_msg->clock = clock;

  /* Broadcast to all clients */
  router_handler->direct_send_to(client_names, msgs);
}

void ServerClientEncode::find_row(
      uint client_id, const RowKey& row_key, uint32_t server_id) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(sc_find_row_msg_t));
  sc_find_row_msg_t *sc_find_row_msg =
      reinterpret_cast<sc_find_row_msg_t *>(msgs[0].data());
  sc_find_row_msg->cmd = FIND_ROW;
  sc_find_row_msg->row_key = row_key;
  sc_find_row_msg->server_id = server_id;

  assert(client_id < client_names.size());
  string client_name = client_names[client_id];
  router_handler->direct_send_to(client_name, msgs);
}

void ServerClientEncode::read_row_batch(
    uint client_id, iter_t data_age, iter_t self_clock,
    uint batch_size, RowKey *row_keys, RowData *row_data) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(3);

  msgs[0].init_size(sizeof(sc_read_row_batch_msg_t));
  sc_read_row_batch_msg_t *sc_read_row_batch_msg =
      reinterpret_cast<sc_read_row_batch_msg_t *>(msgs[0].data());
  sc_read_row_batch_msg->cmd = READ_ROW_BATCH;
  sc_read_row_batch_msg->data_age = data_age;
  sc_read_row_batch_msg->self_clock = self_clock;

  msgs[1].pack_memory(row_keys, sizeof(RowKey) * batch_size);
  msgs[2].pack_memory(row_data, sizeof(RowData) * batch_size);

  assert(client_id < client_names.size());
  string client_name = client_names[client_id];
  router_handler->direct_send_to(client_name, msgs);
}

void ServerClientEncode::get_stats(uint client_id, const string& stats) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(2);

  msgs[0].init_size(sizeof(sc_get_stats_msg_t));
  sc_get_stats_msg_t *sc_get_stats_msg =
      reinterpret_cast<sc_get_stats_msg_t *>(msgs[0].data());
  sc_get_stats_msg->cmd = GET_STATS;

  msgs[1].pack_string(stats);

  assert(client_id < client_names.size());
  string client_name = client_names[client_id];
  router_handler->direct_send_to(client_name, msgs);
}

string ServerClientEncode::get_router_stats() {
  return router_handler->get_stats();
}

void ServerClientEncode::server_started(uint server_id) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(sc_server_started_msg_t));
  sc_server_started_msg_t *sc_server_started_msg =
      reinterpret_cast<sc_server_started_msg_t *>(msgs[0].data());
  sc_server_started_msg->cmd = SERVER_STARTED;
  sc_server_started_msg->server_id = server_id;

  /* Broadcast to all clients */
  router_handler->direct_send_to(client_names, msgs);
}
