#ifndef __client_stats_hpp__
#define __client_stats_hpp__

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

/* Performance tracking and reporting */

#include <stdint.h>

#include <string>

#include "common/internal-config.hpp"

using std::string;

struct Stats {
  int64_t num_threads;
  int64_t row_count;
  int64_t row_count_local;
  int64_t num_thread_hit;

  double read_time;
  double read_get_entry_time;
  double read_wait_miss_time;
  double increment_time;
  double update_get_entry_time;
  double update_apply_proc_cache_time;
  double iterate_time;
  double iter_flush_log_time;
  double iter_var_time;

  double vi_time;

  int64_t total_read;
  int64_t total_increment;
  int64_t total_apply_thr_cache;
  int64_t total_apply_proc_cache;
  int64_t num_thread_cache;
  int64_t num_thread_cache_max;
  int64_t max_total_access;

  string router_stats;
  string bgthread_stats;
  bool server_stats_refreshed;
  string server_stats;

  void reset() {
    row_count = 0;
    row_count_local = 0;
    num_thread_hit = 0;
    read_time = 0.0;
    read_get_entry_time = 0.0;
    read_wait_miss_time = 0.0;
    increment_time = 0.0;
    update_get_entry_time = 0.0;
    update_apply_proc_cache_time = 0.0;
    iterate_time = 0.0;
    iter_flush_log_time = 0.0;
    iter_var_time = 0.0;
    total_read = 0;
    total_increment = 0;
    total_apply_thr_cache = 0;
    total_apply_proc_cache = 0;
  }

  Stats() {
    num_threads = 0;
    num_thread_cache = 0;
    num_thread_cache_max = 0;
    vi_time = 0;
    reset();
  }

  Stats& operator += (const Stats& rhs) {
    num_thread_hit += rhs.num_thread_hit;

    read_time += rhs.read_time;
    read_get_entry_time += rhs.read_get_entry_time;
    read_wait_miss_time += rhs.read_wait_miss_time;

    increment_time += rhs.increment_time;
    update_get_entry_time += rhs.update_get_entry_time;
    update_apply_proc_cache_time += rhs.update_apply_proc_cache_time;

    iterate_time += rhs.iterate_time;
    iter_flush_log_time += rhs.iter_flush_log_time;

    vi_time += rhs.vi_time;

    total_read += rhs.total_read;
    total_increment += rhs.total_increment;
    total_apply_thr_cache += rhs.total_apply_thr_cache;
    total_apply_proc_cache += rhs.total_apply_proc_cache;
    num_thread_cache += rhs.num_thread_cache;
    num_thread_cache_max =
        rhs.num_thread_cache_max > num_thread_cache_max ?
          rhs.num_thread_cache_max : num_thread_cache_max;
    return *this;
  }

  std::string to_json() {
    std::stringstream ss;
    ss << "{ "
       << "\"num_threads\": " << num_threads << ", "
       << "\"row_count\": " << row_count << ", "
       << "\"row_count_local\": " << row_count_local << ", "
       << "\"num_thread_cache\": " << num_thread_cache << ", "
       << "\"num_thread_cache_max\": " << num_thread_cache_max << ", "
       << "\"num_thread_hit\": " << num_thread_hit << ", "
       << "\"READ_TIMING_FREQ\": " << READ_TIMING_FREQ << ", "
       << "\"read_time\": "
       << read_time * READ_TIMING_FREQ / num_threads << ", "
       << "\"read_get_entry_time\": "
       << read_get_entry_time * READ_TIMING_FREQ / num_threads << ", "
       << "\"read_wait_miss_time\": "
       << read_wait_miss_time * READ_TIMING_FREQ / num_threads << ", "
       << "\"INCREMENT_TIMING_FREQ\": "
       << INCREMENT_TIMING_FREQ << ", "
       << "\"increment_time\": "
       << increment_time * INCREMENT_TIMING_FREQ / num_threads << ", "
       << "\"update_get_entry_time\": "
       << update_get_entry_time * INCREMENT_TIMING_FREQ / num_threads << ", "
       << "\"update_apply_proc_cache_time\": "
       << update_apply_proc_cache_time * INCREMENT_TIMING_FREQ / num_threads << ", "
       << "\"iterate_time\": " << iterate_time / num_threads << ", "
       << "\"iter_flush_log_time\": "
       << iter_flush_log_time / num_threads << ", "
       << "\"iter_var_time\": " << iter_var_time << ", "
       << "\"vi_time\": " << vi_time / num_threads << ", "

       << "\"total_read\": " << total_read << ", "
       << "\"total_increment\": " << total_increment << ", "
       << "\"total_apply_thr_cache\": " << total_apply_thr_cache << ", "
       << "\"total_apply_proc_cache\": " << total_apply_proc_cache << ", "

       << "\"router_stats\": " << router_stats << ", "
       << "\"bgthread_stats\": " << bgthread_stats << ", "
       << "\"server_stats\": " << server_stats
       << "}";
    return ss.str();
  }
};

struct BgthreadStats {
  int64_t total_set_row;
  int64_t set_row_num_apply_oplog;
  double total_set_row_time;
  double total_push_updates_time;

  void reset() {
    total_set_row = 0;
    set_row_num_apply_oplog = 0;
    total_set_row_time = 0.0;
    total_push_updates_time = 0.0;
  }

  BgthreadStats() {
    reset();
  }

  BgthreadStats& operator += (const BgthreadStats& rhs) {
    assert(0);
    return *this;
  }

  string to_json() {
    std::stringstream ss;
    ss << "{"
       << "\"SET_ROW_TIMING_FREQ\": " << SET_ROW_TIMING_FREQ << ", "
       << "\"total_set_row\": " << total_set_row << ", "
       << "\"set_row_num_apply_oplog\": " << set_row_num_apply_oplog << ", "
       << "\"total_set_row_time\": "
       << total_set_row_time * SET_ROW_TIMING_FREQ << ", "
       << "\"total_push_updates_time\": " << total_push_updates_time
       << " } ";
    return ss.str();
  }
};


struct ServerStats {
  int64_t num_send;
  int64_t num_update;
  int64_t num_local_update;
  int64_t num_rows;
  double iterate_time;
  double iter_var_time;
  double update_time;

  void reset() {
    num_send = 0;
    num_update = 0;
    num_local_update = 0;
    iterate_time = 0.0;
    update_time = 0.0;
    iter_var_time = 0.0;
  }

  ServerStats() {
    reset();
  }

  ServerStats& operator += (const ServerStats& rhs) {
    return *this;
  }
  std::string to_json() {
    std::stringstream ss;
    ss << "{"
       << "\"num_rows\": " << num_rows << ", "
       << "\"num_send\": " << num_send << ", "
       << "\"num_update\": " << num_update << ", "
       << "\"num_local_update\": " << num_local_update << ", "
       << "\"iterate_time\": " << iterate_time << ", "
       << "\"iter_var_time\": " << iter_var_time << ", "
       << "\"update_time\": " << update_time
       << " } ";
    return ss.str();
  }
};

struct RouterStats {
  int64_t total_send;
  int64_t total_local_send;
  int64_t total_receive;
  int64_t total_local_receive;
  RouterStats() {
    total_send = 0;
    total_local_send = 0;
    total_receive = 0;
    total_local_receive = 0;
  }
  RouterStats& operator += (const RouterStats& rhs) {
    return *this;
  }
  string to_json() {
    std::stringstream ss;
    ss << "{"
       << "\"total_send\": " << total_send << ", "
       << "\"total_local_send\": " << total_local_send << ", "
       << "\"total_receive\": " << total_receive << ", "
       << "\"total_local_receive\": " << total_local_receive
       << " } ";
    return ss.str();
  }
};

#endif  // defined __client_stats_hpp__
