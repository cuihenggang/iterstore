#ifndef __iterstore_hpp__
#define __iterstore_hpp__

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

#include <string>
#include <vector>

#include "user-defined-types.hpp"

struct Config {
  /* Number of application worker threads per machine.
   * A good practice is use the same number threads as the number of cores
   * you have on each machine. */
  int num_threads;
  /* Number of machines */
  int num_machines;
  std::vector<std::string> host_list;
  /* Number of communication channels.
   * A good practice is to set the number of communication channels to be
   * HALF of the number of cores on each machine. */
  int num_channels;
  /* The starting port number to use.
   * IterStore will use the ports in the range of
   *   [tcp_base_port, tcp_base_port + num_channels) */
  int tcp_base_port;
  /* SGD update function */
  enum UpdateFunc {
    UPDATE_FUNC_ADD = 0,
    UPDATE_FUNC_STEPSIZE = 1,
    UPDATE_FUNC_ADAGRAD = 2,
    UPDATE_FUNC_ADAREVISION = 3,
  } update_func;
  /* When read_my_writes is set to 1, each worker is always guaranteed to see
   * its own updates. This mode is only supported when update_func is set to
   * UPDATE_FUNC_ADD. */
  int read_my_writes;
  /* Random seed */
  int random_seed;
  /* Threshold for thread caching.
   * Rows with contention probability more than thread_cache_threshold will
   * be thread-cached. */
  double thread_cache_threshold;
  enum ShardingPolicy {
    SHARDING_POLICY_HASH = 1,
    SHARDING_POLICY_LOCALITY = 3,
  } sharding_policy;
  /* The local communication optimization knob.
   * When local_opt is set to 1, the messages sent within the same machine will
   * use the inproc socket.
   * Actually we didn't see much performance improvement of using the inproc
   * socket from using the TCP socket, so we usually just turn it off. */ 
  int local_opt;
  /* The NUMA-aware memory affinity fields.
   * The NUMA-aware memory affinity is turned on when affinity is set to 1.
   * You should only use the memory affinity optimization
   * when your machine has multiple NUMA memory zones. */   
  int affinity;
  int num_cores;
  int num_zones;
  /* Application tunables.
   * The tunables are only used by user-defined functions. */
  Tunable tunable;

  Config() :
      num_threads(0),
      num_machines(0),
      num_channels(1),
      tcp_base_port(9090),
      update_func(UPDATE_FUNC_ADD), read_my_writes(0),
      random_seed(12345678),
      thread_cache_threshold(0.1),
      sharding_policy(SHARDING_POLICY_LOCALITY),
      local_opt(0),
      affinity(0)
      {}
};

/**
 * @brief IterStore library.
 */
class IterStore {
 public:
  /**
   * @brief IterStore constructor.
   *
   * @param machine_id
   * @param config
   *
   * The constructor initializes IterStore.
   * Each machine should create one IterStore instance with exactly
   * the same configs, and corresponding machine_id.
   * Only one IterStore instance should be created for each machine.
   */
  IterStore(int machine_id, const Config& config);

  ~IterStore();

  /**
   * @brief Register the application thread.
   *
   * This function should be called by each of the application worker thread,
   * before doing calling any other IterStore functions.
   * The number of application worker thread per machine should match
   * the one specified in the Config.
   */
  void ThreadStart();

  /**
   * @brief Free the resource of the calling application worker thread.
   *
   * This function can only be called by each application worker thread
   * exactly once, after the training is done.
   */
  void ThreadStop();

  /**
   * @brief Update parameter data.
   *
   * @param row_key
   * @param update
   *    pointer to row update memory
   *
   * Update a row with update.
   */
  void Update(const RowKey& row_key, const RowData *update);

  /**
   * @brief Read parameter data.
   *
   * @param buffer
   *    pointer to the read buffer
   * @param row_key
   * @param slack
   *
   * Read a row to the read buffer, with the "slack".
   * The read operation is guaranteed to see all the updates from all workers
   * up to the clock of (current_clock - 1 - slack).
   * To do BSP, you need to always use a slack of 0.
   */
  void Read(RowData *buffer, const RowKey& row_key, int slack);

  /**
   * @brief Get access to the local data.
   *
   * @param row_key
   *
   * Return the pointer to the memory keeping the local data.
   * The application can directly modify the local data with the pointer.
   * The local data is local to each application thread,
   * and will not be synchronized with other workers.
   *
   * Using IterStore to manage the local data leaves room for
   * future optimizations, but in this version of IterStore,
   * keeping the local data in IterStore brings no difference from
   * keeping it in the application code.
   */
  RowData *LocalAccess(const RowKey& row_key);

  /**
   * @brief Signal the completion of a clock.
   *
   * @param progress
   *
   * This function signals the completion of a clock.
   * All updates of this clock will be pushed to the tablet servers
   * when this function is called.
   *
   * The progress parameter is used for reporting the training loss.
   * The server will aggregate and print the loss from all workers.
   */
  void Iterate(double progress = std::numeric_limits<double>::quiet_NaN());

  /**
   * @brief Report the update operation.
   *
   * @param row_key
   *
   * The virtual iteration functions can only be used before the first clock.
   */
  void VirtualUpdate(const RowKey& row_key);

  /**
   * @brief Report the read operation.
   *
   * @param row_key
   * @param slack
   *
   * The virtual iteration functions can only be used before the first clock.
   */
  void VirtualRead(const RowKey& row_key, int slack);

  /**
   * @brief Report the local access operation.
   *
   * @param row_key
   *
   * The virtual iteration functions can only be used before the first clock.
   */
  void VirtualLocalAccess(const RowKey& row_key);

  /**
   * @brief Report the iterate operation.
   *
   * The virtual iteration functions can only be used before the first clock.
   */
  void VirtualIterate();

  /**
   * @brief Finalize IterStore with the information collected
   *        during virtual iteration.
   *
   * IterStore will apply several optimizations according to the
   * access information collected during the virtual iteration.
   * This function should be called at the end of the virtual iteration,
   * and before the first clock.
   */
  void FinishVirtualIteration();

  /**
   * @brief Get the collected performance statistics.
   *
   * The statistics are returned as a JSON formatted string.
   */
  std::string GetStats();
};

#endif  // defined __iterstore_hpp__
