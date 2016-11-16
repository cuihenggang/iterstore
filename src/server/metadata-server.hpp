#ifndef __metadata_server_hpp__
#define __metadata_server_hpp__

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

using std::string;
using std::vector;

using boost::shared_ptr;

class ServerClientEncode;

class MetadataServer {

  typedef std::vector<RowAccessInfo> AccessInfo;
  struct MachineAccessInfo {
    uint machine_id;
    uint num_read;   /* Read frequency */
    uint num_write;  /* Write frequency */

    MachineAccessInfo(uint t = 0, uint r = 0, uint w = 0) {
      machine_id = t;
      num_read = r;
      num_write = w;
    }
  };
  typedef std::vector<MachineAccessInfo> MachineAccessInfos;
  typedef boost::unordered_map<RowKey, MachineAccessInfos> RowAccessInfoMap;

  struct FindRowRequest {
    uint client;
    RowKey row_key;

    FindRowRequest(uint client, const RowKey& row_key) :
        client(client), row_key(row_key) {}
  };

  typedef boost::unordered_map<RowKey, uint> RowServerMap;

  shared_ptr<ServerClientEncode> communicator;
  uint channel_id;
  uint num_channels;
  uint machine_id;
  uint num_machines;

  bool ready_to_serve;
  uint num_clients_reported;
  std::vector<FindRowRequest> pending_requests;
  std::vector<uint> server_load;
  RowAccessInfoMap row_access_summary_map;
  RowServerMap row_server_map;

  Config config;

 private:
  void decide_data_assignment();
  void serve_pending_requests();

 public:
  MetadataServer(
      shared_ptr<ServerClientEncode> comm,
      uint channel_id, uint num_channels,
      uint machine_id, uint num_machines,
      const Config& config) :
    communicator(comm),
    channel_id(channel_id), num_channels(num_channels),
    machine_id(machine_id), num_machines(num_machines),
    server_load(num_machines),
    config(config) {
    num_clients_reported = 0;
    ready_to_serve = false;
    for (uint i = 0; i < num_machines; i++) {
      server_load[i] = 0;
    }
  }
  void report_access_info(uint client_id, const AccessInfo& access_info);

  void find_row(uint client_id, const RowKey& row_key);
  string get_stats();
};

#endif  // defined __metadata_server_hpp__
