#ifndef __router_handler_hpp__
#define __router_handler_hpp__

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

/* Socket layer that handles ZMQ sockets, both in-process and over the net */

#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>

#include <map>
#include <string>
#include <vector>

#include "common/common-utils.hpp"
#include "common/portable-bytes.hpp"
#include "common/zmq-util.hpp"
#include "common/stats-tracker.hpp"

using std::string;
using std::vector;

class RouterHandler {
 public:
  typedef boost::function<void (
      const string&, vector<ZmqPortableBytes>&)> RecvCallback;

 private:

  uint channel_id;
  boost::shared_ptr<zmq::context_t> zmq_ctx;
  vector<string> connect_to;
  vector<string> bind_to;
  string identity;
  bool client;

  zmq::socket_t router_socket;
  zmq::socket_t shutdown_socket;
  zmq::socket_t pull_socket;
  zmq::socket_t local_recv_socket;

  boost::thread_specific_ptr<zmq::socket_t> snd_msg_socket;
  boost::thread_specific_ptr<zmq::socket_t> local_snd_msg_socket;

  boost::shared_ptr<boost::thread> handler_thread;

  uint numa_node_id;
  Config config;
  RouterStats stats;

 public:
  RouterHandler(
      uint channel_id,
      boost::shared_ptr<zmq::context_t> ctx,
      const vector<string>& connect,
      const vector<string>& bind,
      const string& identity,
      uint numa_node_id,
      const Config& config_i);
  ~RouterHandler();
  void start_handler_thread(RecvCallback recv_callback);
  void do_handler(RecvCallback recv_callback);
  void stop_handler_thread();

  void send_to(
      const string& dest, vector<ZmqPortableBytes>& msgs);
  void send_to(
      const vector<string>& dests, vector<ZmqPortableBytes>& msgs);

  /* WARNING: Only one thread should be allowed to use these methods */
  void direct_send_to(
      const string& dest, vector<ZmqPortableBytes>& msgs);
  void direct_send_to(
      const vector<string>& dests, vector<ZmqPortableBytes>& msgs);

  string get_stats();

};

#endif  // defined __router_handler_hpp__
