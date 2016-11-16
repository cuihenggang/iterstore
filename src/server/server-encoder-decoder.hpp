#ifndef __server_encoder_decoder_hpp__
#define __server_encoder_decoder_hpp__

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

#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

#include <fstream>
#include <vector>
#include <string>
#include <utility>
#include <set>
#include <map>

#include "common/common-utils.hpp"
#include "common/wire-protocol.hpp"
#include "common/zmq-portable-bytes.hpp"
#include "common/router-handler.hpp"
#include "tablet-server.hpp"
#include "metadata-server.hpp"

using std::string;
using std::vector;

using boost::shared_ptr;

/* Encodes messages to client */
class ServerClientEncode {
  shared_ptr<RouterHandler> router_handler;
  vector<string> client_names;

 public:
  explicit ServerClientEncode(
      shared_ptr<RouterHandler> rh, uint num_machines,
      uint server_id, const Config& config)
        : router_handler(rh) {
    for (uint i = 0; i < num_machines; i++) {
      std::string cname("local");
      if (!config.local_opt || i != server_id) {
        cname = (boost::format("client-%i") % i).str();
      }
      client_names.push_back(cname);
    }
  }
  void find_row(
      uint client_id, const RowKey& row_key, uint server_id);
  void read_row_batch(
      uint client_id, iter_t data_age, iter_t self_clock,
      uint batch_size, RowKey *row_keys, RowData *row_data);
  void clock(uint32_t server_id, iter_t clock);
  void get_stats(uint client_id, const string& stats);
  string get_router_stats();
  void server_started(uint server_id);
};

/* Decodes messages from client */
class ClientServerDecode {
  shared_ptr<TabletStorage> storage;
  shared_ptr<MetadataServer> metadata_server;

 public:
  explicit ClientServerDecode(
      shared_ptr<TabletStorage> storage,
      shared_ptr<MetadataServer> metadata_server);
  void find_row(vector<ZmqPortableBytes>& msgs);
  void iterate(vector<ZmqPortableBytes>& msgs);
  void clock_with_updates_batch(
      vector<ZmqPortableBytes>& msgs);
  void subscribe_rows(vector<ZmqPortableBytes>& msgs);
  void report_access_info(vector<ZmqPortableBytes>& msgs);
  void get_stats(vector<ZmqPortableBytes>& msgs);
  void worker_started(vector<ZmqPortableBytes>& msgs);
  void report_progress(vector<ZmqPortableBytes>& msgs);
  void decode_msg(vector<ZmqPortableBytes>& msgs);
  void router_callback(const string& src, vector<ZmqPortableBytes>& msgs);

  RouterHandler::RecvCallback get_recv_callback();
};

#endif  // defined __server_encoder_decoder_hpp__
