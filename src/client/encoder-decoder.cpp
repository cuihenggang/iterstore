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

/* Encode and decode messages for the client */

#include <string>
#include <vector>

#include "common/work-puller.hpp"
#include "common/background-worker.hpp"
#include "encoder-decoder.hpp"

using std::string;
using std::vector;
using std::cerr;
using std::endl;
using boost::shared_ptr;
using boost::make_shared;

void ClientServerEncode::find_row(
    const RowKey& row_key, uint metadata_sever_id) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(cs_find_row_msg_t));
  cs_find_row_msg_t *cs_find_row_msg =
    reinterpret_cast<cs_find_row_msg_t *>(msgs[0].data());
  cs_find_row_msg->cmd = FIND_ROW;
  cs_find_row_msg->client_id = client_id;
  cs_find_row_msg->row_key = row_key;

  /* Currently, the tablet servers are also metadata servers */
  string metadata_sever_name = server_names[metadata_sever_id];

  router_handler->send_to(metadata_sever_name, msgs);
}

void ClientServerEncode::subscribe_rows(
    uint server_id, RowKeys& row_keys) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(2);

  msgs[0].init_size(sizeof(cs_subscribe_rows_msg_t));
  cs_subscribe_rows_msg_t *cs_subscribe_rows_msg =
    reinterpret_cast<cs_subscribe_rows_msg_t *>(msgs[0].data());
  cs_subscribe_rows_msg->cmd = SUBSCRIBE_ROWS;
  cs_subscribe_rows_msg->client_id = client_id;

  msgs[1].pack_vector<RowKey>(row_keys);

  CHECK_LT(server_id, server_names.size());
  string server_name = server_names[server_id];
  router_handler->send_to(server_name, msgs);
}

void ClientServerEncode::clock(uint server_id, iter_t clock) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(cs_iterate_msg_t));
  cs_iterate_msg_t *cs_iterate_msg =
    reinterpret_cast<cs_iterate_msg_t *>(msgs[0].data());
  cs_iterate_msg->cmd = ITERATE;
  cs_iterate_msg->client_id = client_id;
  cs_iterate_msg->clock = clock;

  CHECK_LT(server_id, server_names.size());
  string server_name = server_names[server_id];

  router_handler->send_to(server_name, msgs);
}

void ClientServerEncode::clock_broadcast(iter_t clock) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(cs_iterate_msg_t));
  cs_iterate_msg_t *cs_iterate_msg =
    reinterpret_cast<cs_iterate_msg_t *>(msgs[0].data());
  cs_iterate_msg->cmd = ITERATE;
  cs_iterate_msg->client_id = client_id;
  cs_iterate_msg->clock = clock;

  /* Broadcast to all tablet servers */
  router_handler->send_to(server_names, msgs);
}

void ClientServerEncode::clock_with_updates_batch(
    uint server_id, iter_t clock,
    uint batch_size, RowKey *row_keys, RowData *row_updates) {
  if (batch_size == 0) {
    this->clock(server_id, clock);
    return;
  }

  vector<ZmqPortableBytes> msgs;
  msgs.resize(3);

  msgs[0].init_size(sizeof(cs_clock_with_updates_batch_msg_t));
  cs_clock_with_updates_batch_msg_t *cs_clock_with_updates_batch_msg =
    reinterpret_cast<cs_clock_with_updates_batch_msg_t *>(msgs[0].data());
  cs_clock_with_updates_batch_msg->cmd = CLOCK_WITH_UPDATES_BATCH;
  cs_clock_with_updates_batch_msg->client_id = client_id;
  cs_clock_with_updates_batch_msg->clock = clock;

  msgs[1].pack_memory(row_keys, sizeof(RowKey) * batch_size);
  msgs[2].pack_memory(row_updates, sizeof(RowData) * batch_size);

  CHECK_LT(server_id, server_names.size());
  string server_name = server_names[server_id];
  router_handler->send_to(server_name, msgs);
}

void ClientServerEncode::report_access_info(
      uint metadata_server_id, const std::vector<RowAccessInfo>& access_info) {
  /* Currently, the tablet servers are also metadata servers */
  assert(metadata_server_id < server_names.size());
  string metadata_sever_name = server_names[metadata_server_id];

  vector<ZmqPortableBytes> msgs;
  msgs.resize(2);

  msgs[0].init_size(sizeof(cs_report_access_info_msg_t));
  cs_report_access_info_msg_t *cs_report_access_info_msg =
    reinterpret_cast<cs_report_access_info_msg_t *>(msgs[0].data());
  cs_report_access_info_msg->cmd = REPORT_ACCESS_INFO;
  cs_report_access_info_msg->client_id = client_id;

  msgs[1].pack_vector<RowAccessInfo>(access_info);

  router_handler->send_to(metadata_sever_name, msgs);
}

void ClientServerEncode::get_stats(uint server_id) {
  CHECK_LT(server_id, server_names.size());
  string sever_name = server_names[server_id];

  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(cs_get_stats_msg_t));
  cs_get_stats_msg_t *cs_get_stats_msg =
    reinterpret_cast<cs_get_stats_msg_t *>(msgs[0].data());
  cs_get_stats_msg->cmd = GET_STATS;
  cs_get_stats_msg->client_id = client_id;

  router_handler->send_to(sever_name, msgs);
}

void ClientServerEncode::worker_started() {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(cs_worker_started_msg_t));
  cs_worker_started_msg_t *cs_worker_started_msg =
      reinterpret_cast<cs_worker_started_msg_t *>(msgs[0].data());
  cs_worker_started_msg->cmd = WORKER_STARTED;
  cs_worker_started_msg->client_id = client_id;

  router_handler->send_to(server_names, msgs);
}

void ClientServerEncode::report_progress(
    int clock, double progress) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);

  msgs[0].init_size(sizeof(cs_report_progress_msg_t));
  cs_report_progress_msg_t *cs_report_progress_msg =
      reinterpret_cast<cs_report_progress_msg_t *>(msgs[0].data());
  cs_report_progress_msg->cmd = REPORT_PROGRESS;
  cs_report_progress_msg->client_id = client_id;
  cs_report_progress_msg->clock = clock;
  cs_report_progress_msg->progress = progress;

  router_handler->send_to(server_names, msgs);
}

void ClientServerEncode::broadcast_msg(vector<ZmqPortableBytes>& msgs) {
  /* Make a copy of the message for sending */
  vector<ZmqPortableBytes> msgs_copy(msgs.size());
  for (uint j = 0; j < msgs_copy.size(); j ++) {
    msgs_copy[j].copy(msgs[j]);
  }
  /* Broadcast to all servers */
  router_handler->send_to(server_names, msgs_copy);
}


ServerClientDecode::ServerClientDecode(
        uint channel_id, shared_ptr<zmq::context_t> ctx,
        ClientLib *client_lib, bool work_in_bg,
        uint numa_node_id, const Config& config)
  : channel_id(channel_id), zmq_ctx(ctx),
    client_lib(client_lib), work_in_background(work_in_bg),
    numa_node_id(numa_node_id), config(config) {
  if (work_in_background) {
    /* Start background worker thread */
    string endpoint = "inproc://bg-recv-worker";
    shared_ptr<WorkPuller> work_puller =
      make_shared<WorkPuller>(zmq_ctx, endpoint);
    BackgroundWorker::WorkerCallback worker_callback =
      bind(&ServerClientDecode::decode_msg, this, _1);
    BackgroundWorker bg_worker(channel_id, work_puller, numa_node_id, config);
    bg_worker.add_callback(DECODE_CMD, worker_callback);
    bg_decode_worker_thread = make_shared<boost::thread>(bg_worker);

    /* Init work pusher */
    decode_work_pusher = make_shared<WorkPusher>(zmq_ctx, endpoint);
  }
}

void ServerClientDecode::find_row(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(sc_find_row_msg_t));

  sc_find_row_msg_t *sc_find_row_msg =
    reinterpret_cast<sc_find_row_msg_t *>(args[0].data());
  RowKey& row_key = sc_find_row_msg->row_key;
  uint server_id = sc_find_row_msg->server_id;
  client_lib->find_row_cbk(row_key, server_id);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ServerClientDecode::read_row_batch(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 3);
  CHECK_EQ(args[0].size(), sizeof(sc_read_row_batch_msg_t));

  sc_read_row_batch_msg_t *sc_read_row_batch_msg =
    reinterpret_cast<sc_read_row_batch_msg_t *>(args[0].data());
  iter_t data_age = sc_read_row_batch_msg->data_age;
  iter_t self_clock = sc_read_row_batch_msg->self_clock;

  RowKey *row_keys =
      reinterpret_cast<RowKey *>(args[1].data());
  uint batch_size = args[1].size() / sizeof(RowKey);
  RowData *row_datas =
      reinterpret_cast<RowData *>(args[2].data());
  CHECK_EQ(batch_size, args[2].size() / sizeof(RowData));
  client_lib->recv_row_batch(
      channel_id, data_age, self_clock,
      row_keys, row_datas, batch_size);
  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ServerClientDecode::iterate(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(sc_iterate_msg_t));

  sc_iterate_msg_t *sc_iterate_msg =
    reinterpret_cast<sc_iterate_msg_t *>(args[0].data());
  uint server_id = sc_iterate_msg->server_id;
  iter_t clock = sc_iterate_msg->clock;

  client_lib->server_iterate(channel_id, server_id, clock);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ServerClientDecode::get_stats(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 2);

  string stats;
  args[1].unpack_string(stats);
  client_lib->get_stats_cbk(stats);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ServerClientDecode::server_started(vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  CHECK_EQ(args[0].size(), sizeof(sc_server_started_msg_t));

  sc_server_started_msg_t *sc_server_started_msg =
      reinterpret_cast<sc_server_started_msg_t *>(args[0].data());
  uint server_id = sc_server_started_msg->server_id;

  client_lib->server_started(channel_id, server_id);

  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

void ServerClientDecode::decode_msg(vector<ZmqPortableBytes>& msgs) {
  CHECK_GE(msgs.size(), 1);
  CHECK_GE(msgs[0].size(), sizeof(command_t));

  command_t cmd;
  msgs[0].unpack<command_t>(cmd);
  switch (cmd) {
    break;
  case FIND_ROW:
    find_row(msgs);
    break;
  case READ_ROW_BATCH:
    read_row_batch(msgs);
    break;
  case ITERATE:
    iterate(msgs);
    break;
  case GET_STATS:
    get_stats(msgs);
    break;
  case SERVER_STARTED:
    server_started(msgs);
    break;
  default:
    cerr << "Client received unknown command!" << endl;
    assert(0);
  }
}

void ServerClientDecode::router_callback(
    const string& src, vector<ZmqPortableBytes>& msgs) {
  /* The "src" field is not send to the background worker, because we don't
   * want to construct another string vector object. */
  if (work_in_background) {
    /* Push to the background thread */
    decode_work_pusher->push_work(DECODE_CMD, msgs);
  } else {
    /* Do it myself */
    decode_msg(msgs);
  }
}

RouterHandler::RecvCallback ServerClientDecode::get_recv_callback() {
  return bind(&ServerClientDecode::router_callback, this, _1, _2);
}

void ServerClientDecode::stop_decoder() {
  if (work_in_background) {
    /* Shut down background worker thread */
    vector<ZmqPortableBytes> args;  /* Args is empty */
    decode_work_pusher->push_work(BackgroundWorker::STOP_CMD, args);
    (*bg_decode_worker_thread).join();

    /* Set "work_in_background" to false, so that we won't do that again. */
    work_in_background = false;
  }
}

ServerClientDecode::~ServerClientDecode() {
  stop_decoder();
}
