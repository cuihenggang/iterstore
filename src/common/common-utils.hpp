#ifndef __common_utils_hpp__
#define __common_utils_hpp__

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

#include <set>
#include <string>
#include <vector>
#include <list>
#include <queue>

#include <boost/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>

#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>

#include <glog/logging.h>

#include "iterstore.hpp"
#include "common/internal-config.hpp"
#include "common/affinity.hpp"
#include "common/hopscotch_map.hpp"

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;

using boost::shared_ptr;
using boost::make_shared;

typedef int iter_t;

typedef unsigned int uint;

typedef std::vector<RowKey> RowKeys;
typedef std::vector<RowData> RowDatas;

typedef std::vector<double> DoubleVec;

typedef std::vector<int> VecClock;
typedef tbb::concurrent_unordered_map<uint, int> ConcurrentUnorderedMapClock;

typedef boost::unique_lock<boost::mutex> ScopedLock;

typedef tbb::atomic<bool> atomic_bool;
typedef tbb::atomic<uint> atomic_uint;
typedef tbb::atomic<int> atomic_int;

/* Define the hash function, for the unordered_maps to work */
namespace boost {
  template <>
  struct hash<RowKey> {
    size_t operator()(const RowKey& row_key) const {
        return row_key.get_hash();
    }
  };
}

inline int clock_min(VecClock clocks) {
  CHECK(clocks.size());
  int cmin = clocks[0];
  for (uint i = 1; i < clocks.size(); i++) {
    cmin = clocks[i] < cmin ? clocks[i] : cmin;
  }
  return cmin;
}

inline int clock_max(VecClock clocks) {
  CHECK(clocks.size());
  int cmax = clocks[0];
  for (uint i = 1; i < clocks.size(); i++) {
    cmax = clocks[i] > cmax ? clocks[i] : cmax;
  }
  return cmax;
}

inline int clock_max(const ConcurrentUnorderedMapClock& cum_clock) {
  bool init = false;
  int clock = 0;
  for (ConcurrentUnorderedMapClock::const_iterator it = cum_clock.begin();
       it != cum_clock.end(); it++) {
    if (!init || it->second > clock) {
      clock = it->second;
      init = true;
    }
  }
  return clock;
}

inline int clock_min(const ConcurrentUnorderedMapClock& cum_clock) {
  bool init = false;
  int clock = 0;
  for (ConcurrentUnorderedMapClock::const_iterator it = cum_clock.begin();
       it != cum_clock.end(); it++) {
    if (!init || it->second < clock) {
      clock = it->second;
      init = true;
    }
  }
  return clock;
}

inline uint get_nearest_power2(uint n) {
  uint power2 = 1;
  while (power2 < n) {
    power2 <<= 1;
  }
  return power2;
}

inline double get_interception(DoubleVec b, double bt) {
  CHECK_GE(b.size(), 2);
  double b_front = b.front();
  double b_back = b.back();
  uint i;
  if (b_front > b_back) {
    if (b_front <= bt || b_back >= bt) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    for (i = 0; i < b.size(); i++) {
      if (b[i] < bt) {
        break;
      }
    }
  } else if (b_front < b_back) {
    if (b_front >= bt || b_back <= bt) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    for (i = 0; i < b.size(); i++) {
      if (b[i] > bt) {
        break;
      }
    }
  } else {
    return std::numeric_limits<double>::quiet_NaN();
  }
  CHECK_GE(i, 1);
  double a1 = static_cast<double>(i - 1);
  double a2 = static_cast<double>(i);
  double b1 = b[i - 1];
  double b2 = b[i];
  double at = a1 + (a2 - a1) / (b2 - b1) * (bt - b1);
  return at;
}

#endif  // defined __common_utils_hpp__