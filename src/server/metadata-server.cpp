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

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

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

void MetadataServer::report_access_info(
      uint client_id, const AccessInfo& access_info) {
  if (config.sharding_policy == Config::SHARDING_POLICY_LOCALITY) {
    for (uint i = 0; i < access_info.size(); i++) {
      const RowAccessInfo& row_access_info = access_info[i];
      MachineAccessInfo machine_access_info(
          client_id, row_access_info.num_read, row_access_info.num_write);
      row_access_summary_map[row_access_info.row_key].
          push_back(machine_access_info);
    }

    num_clients_reported++;
    if (num_clients_reported == num_machines) {
      /* Received access info from all clients, now the row-to-tablet mapping
       * should be stable, and we can service FIND_ROW requests */
      decide_data_assignment();
      ready_to_serve = true;
      serve_pending_requests();
    }
  }

  /* We also view that as an automatic FIND_ROW request */
  for (uint i = 0; i < access_info.size(); i++) {
    const RowAccessInfo& row_access_info = access_info[i];
    find_row(client_id, row_access_info.row_key);
  }
}

void MetadataServer::decide_data_assignment() {
  for (RowAccessInfoMap::iterator
      it = row_access_summary_map.begin();
      it != row_access_summary_map.end(); it++) {
    MachineAccessInfos& machine_access_infos = it->second;
    uint max_freq = 0;
    uint chosen_server_id = 0;
    for (uint i = 0; i < machine_access_infos.size(); i++) {
      MachineAccessInfo& machine_access_info = machine_access_infos[i];
      uint access_freq =
          machine_access_info.num_read + machine_access_info.num_write;
      uint server_id = machine_access_info.machine_id;
      if (access_freq > max_freq ||
        (access_freq == max_freq &&
        server_load[server_id] < server_load[chosen_server_id])) {
          max_freq = access_freq;
          chosen_server_id = server_id;
      }
    }
    row_server_map[it->first] = chosen_server_id;
    server_load[chosen_server_id] += max_freq;
  }
}

void MetadataServer::serve_pending_requests() {
  for (uint i = 0; i < pending_requests.size(); i ++) {
    FindRowRequest& request = pending_requests[i];
    RowServerMap::iterator it = row_server_map.find(request.row_key);
    if (it != row_server_map.end()) {
      uint server_id = it->second;
      communicator->find_row(
          request.client, request.row_key, server_id);
    } else {
      CHECK(0);
    }
  }
}

void MetadataServer::find_row(
    uint client_id, const RowKey& row_key) {
  switch (config.sharding_policy) {
    case Config::SHARDING_POLICY_HASH: {
      /* tablet server <-- row_id % num_machines */
      uint server_id =
          row_key.get_hash() % (num_machines * num_channels) / num_channels;
      communicator->find_row(client_id, row_key, server_id);
      break;
    }
    case Config::SHARDING_POLICY_LOCALITY: {
      /* max(local access) + load balancing */
      if (ready_to_serve) {
        RowServerMap::iterator it = row_server_map.find(row_key);
        if (it != row_server_map.end()) {
          uint server_id = it->second;
          communicator->find_row(client_id, row_key, server_id);
        } else {
          CHECK(0);
        }
      } else {
        /* Row-to-tablet mapping is not ready, save to pending requests */
        pending_requests.push_back(FindRowRequest(client_id, row_key));
      }
      break;
    }
    default: {
      CHECK(0) << config.sharding_policy;
    }
  }
}

string MetadataServer::get_stats() {
  return "";
}
