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

#include <boost/program_options.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include <iostream>
#include <string>
#include <vector>

#include "iterstore.hpp"

#include "parse-config-file.hpp"
#include "mlr.hpp"

namespace po = boost::program_options;

using std::string;
using std::vector;
using boost::shared_ptr;
using boost::make_shared;

int main(int argc, char* argv[]) {
  int machine_id;
  Config config;
  string hostfile;
  string format;
  string datafile;
  int num_iterations;
  int update_func;
  int sharding_policy;
  double learning_rate;
  double decay_rate;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("machine_id",
     po::value<int>(&machine_id),
     "")
    ("hostfile",
     po::value<string>(&hostfile),
     "")
    ("threads",
     po::value<int>(&config.num_threads),
     "")
    ("sharding_policy", po::value<int>(&sharding_policy)->default_value(3),
     "")
    ("num_channels", po::value<int>(&config.num_channels)->default_value(1),
     "")
    ("read_my_writes", po::value<int>(&config.read_my_writes)->default_value(1),
     "")
    ("update_func", po::value<int>(&update_func)->default_value(0),
     "")
    ("random_seed", po::value<int>(&config.random_seed)->default_value(12345678),
     "")
    ("tcp_base_port", po::value<int>(&config.tcp_base_port)->default_value(9090),
     "")
    ("learning_rate", po::value<val_t>(&config.tunable.learning_rate)->default_value(0.01),
     "")
    ("slack", po::value<int>(&config.tunable.slack)->default_value(0),
     "")
    ("format",
     po::value<string>(&format)->default_value("partbin"),
     "Format of data file")
    ("data",
     po::value<string>(&datafile),
     "Data file")
    ("iters",
     po::value<int>(&num_iterations),
     "Number of iterations")
    ("learning_rate",
      po::value<double>(&learning_rate)->default_value(0.4),
      "Initial step size")
    ("decay_rate",
      po::value<double>(&decay_rate)->default_value(0.95),
      "multiplicative decay");

  parse_command_line_and_config_file(argc, argv, desc);
  if (machine_id == 0) {
    print_parsed_options();
  }

  /* Parse hostfile and start parameter server library */
  parse_hostfile(hostfile, config.host_list);
  config.update_func = static_cast<Config::UpdateFunc>(update_func);
  config.sharding_policy = static_cast<Config::ShardingPolicy>(sharding_policy);
  config.num_machines = config.host_list.size();
  shared_ptr<IterStore> ps = make_shared<IterStore>(machine_id, config);

  /* Launch the application worker threads */
  boost::thread_group workers_executer;
  int num_workers = config.num_machines * config.num_threads;
  for (int i = 0; i < config.num_threads; i++) {
    int worker_id = machine_id + i * config.num_machines;
    MLRWorker app_worker(
        ps, worker_id, num_workers,
        config.tunable, config.random_seed,
        num_iterations, 
        format, datafile,
        learning_rate, decay_rate);
    workers_executer.create_thread(app_worker);
  }

  /* Finish and exit */
  workers_executer.join_all();
}
