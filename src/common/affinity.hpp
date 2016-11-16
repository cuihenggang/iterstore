#ifndef __AFFINITY_HPP__
#define __AFFINITY_HPP__

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

/* Helper functions for setting affinities */

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sched.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <sys/syscall.h>
#define gettid() syscall(__NR_gettid)
#include <numa.h>
#include <numaif.h>

typedef unsigned int uint;

inline void set_cpu_affinity(uint node_id, uint num_cores, uint num_zones) {
  int rc = 0;
  cpu_set_t mask;
  CPU_ZERO(&mask);
  uint node_size = num_cores / num_zones;
  for (uint i = node_id * node_size; i < (node_id + 1) * node_size; i++) {
    CPU_SET(i, &mask);
  }
  rc = sched_setaffinity(gettid(), sizeof(mask), &mask);
  assert(rc == 0);
}

inline void set_mem_affinity(uint node_id) {
  struct bitmask *mask = numa_allocate_nodemask();
  mask = numa_bitmask_setbit(mask, node_id);
  numa_set_bind_policy(1); /* set NUMA zone binding to be strict */
  numa_set_membind(mask);
  numa_free_nodemask(mask);
}

inline uint get_numa_node_id(uint entity_id, uint num_entity, uint num_nodes) {
  uint entity_per_node;
  if (num_entity < num_nodes) {
    entity_per_node = 1;
  } else {
    assert(num_entity % num_nodes == 0);
    entity_per_node = num_entity / num_nodes;
  }
  uint node_id = entity_id / entity_per_node;
  return node_id;
}

inline void set_affinity(uint node_id, uint num_cores, uint num_nodes) {
  set_cpu_affinity(node_id, num_cores, num_nodes);
  set_mem_affinity(node_id);
}

#endif  // __AFFINITY_HPP__
