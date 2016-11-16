#ifndef __user_defined_types_hpp__
#define __user_defined_types_hpp__

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

/* You will define your Key, Value, and Tunable types in this file */

#include <string>
#include <vector>
#include <utility>

#include <boost/unordered_map.hpp>

#include <glog/logging.h>

using std::vector;
using std::string;
using std::cout;
using std::cerr;
using std::endl;

/**
 * @brief User-defined type for row key.
 *
 * For many of our optimizations to work, we hope the row keys can be stored
 * as vector<RowKey> in contiguous memory. This means that we assume each
 * row_key of type RowKey is stored in a piece of contiguous memory,
 * starting from &row_key, with size sizeof(RowKey).
 *
 * Because we will use RowKey as the key type of unordered_maps,
 * the RowKey type needs to be defined with a "==" operator
 * and a get_hash() function.
 */
struct RowKey {
  size_t table;
  size_t row;
  RowKey() {}
  RowKey(size_t table, size_t row) :
      table(table), row(row) {}
  bool operator == (const RowKey &other) const {
    return (table == other.table && row == other.row);
  }
  size_t get_hash() const {
    return row;
  }
};

/**
 * @brief User-defined type for row data.
 *
 * For many of our optimizations to work, we hope the data of multiple rows can
 * be stored as vector<RowData> in contiguous memory. So we assume each
 * row_data of type RowData is stored in a piece of contiguous memory,
 * starting from &row_data, with size sizeof(RowData).
 *
 * The RowData type should also be defined with a "+=" operator
 * and an init() function to zerofy the data.
 */
typedef float val_t;
#define ROW_DATA_SIZE 500
struct RowData {
  val_t data[ROW_DATA_SIZE];
  void init() {
    for (int i = 0; i < ROW_DATA_SIZE; i++) {
      data[i] = 0;
    }
  }
  RowData() {
    init();
  }
  RowData& operator += (const RowData& right) {
    for (int i = 0; i < ROW_DATA_SIZE; i++) {
      data[i] += right.data[i];
    }
    return *this;
  }
};

struct Tunable {
  val_t learning_rate;
  int slack;
  Tunable() {}
  Tunable(const Tunable& other) :
      learning_rate(other.learning_rate),
      slack(other.slack) {}
};

#endif  // defined __user_defined_types_hpp__
