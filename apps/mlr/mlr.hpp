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

#ifndef MLR_HPP_
#define MLR_HPP_

#include <stdint.h>
#include <boost/unordered_set.hpp>
#include <boost/format.hpp>

#include <cmath>
#include <sstream>
#include <string>
#include <vector>
#include <utility>
#include <limits>
#include <fstream>

#include <glog/logging.h>

#include <tbb/tick_count.h>

#include "iterstore.hpp"
#include "mlr-util.hpp"
#include "metafile-reader.hpp"

#define WEIGHT_TABLE  0

using std::string;
using std::cout;
using std::cerr;
using std::endl;
using std::istringstream;


class MLRWorker {
 public:
  shared_ptr<IterStore> ps_;
  int worker_id_;
  int num_workers_;
  Tunable tunable_;
  int random_seed_;
  uint num_iterations_;
  string format_;
  string data_file_;
  float learning_rate_;
  float decay_rate_;

  uint num_train_data_;
  uint feature_dim_;
  uint num_labels_;
  string read_format_;
  bool feature_one_based_;
  bool label_one_based_;
  bool snappy_compressed_;

  uint cur_clock_;
  uint batch_offset_;
  uint batch_size_;

  vector<RowData> train_features_;
  vector<int> train_labels_;
  vector<RowData> w_cache_;
  vector<RowData> w_delta_;

  void read_metafile(string datafile_prefix) {
    string meta_file = datafile_prefix + ".meta";
    MetafileReader mreader(meta_file);
    num_train_data_ = mreader.get_int32("num_train_this_partition");
    feature_dim_ = mreader.get_int32("feature_dim");
    num_labels_ = mreader.get_int32("num_labels");
    read_format_ = mreader.get_string("format");
    feature_one_based_ = mreader.get_bool("feature_one_based");
    label_one_based_ = mreader.get_bool("label_one_based");
    snappy_compressed_ = mreader.get_bool("snappy_compressed");
    assert(feature_dim_ <= ROW_DATA_SIZE);
  }

  /* Read data */
  void read_data(string datafile_prefix) {
    read_metafile(datafile_prefix);
    string train_file = datafile_prefix
      + (1 ? "" : "." + boost::lexical_cast<uint>(worker_id_));
    vector<vector<float> > train_features_tmp;
    if (read_format_ == "bin") {
      ReadDataLabelBinary(train_file, feature_dim_, num_train_data_,
          &train_features_tmp, &train_labels_, feature_one_based_,
          label_one_based_);
    } else if (read_format_ == "libsvm") {
      ReadDataLabelLibSVM(train_file, feature_dim_, num_train_data_,
          &train_features_tmp, &train_labels_, feature_one_based_,
          label_one_based_, snappy_compressed_);
    }
    train_features_.resize(train_features_tmp.size());
    for (uint i = 0; i < train_features_tmp.size(); i++) {
      copy_vector_to_row_data(
        train_features_[i], train_features_tmp[i], feature_dim_);
    }

    uint div = train_features_.size() / num_workers_;
    uint res = train_features_.size() % num_workers_;
    batch_offset_ = div * worker_id_ + (res > worker_id_ ? worker_id_ : res);
    batch_size_ = div + (res > worker_id_ ? 1 : 0);
  }

  void initialize() {
    w_cache_.resize(num_labels_);
    w_delta_.resize(num_labels_);
    /* TODO: initialize parameter data */
    ps_->Iterate(0.0);
  }

  void virtual_iteration() {
    for (uint i = 0; i < num_labels_; ++i) {
      ps_->VirtualRead(RowKey(WEIGHT_TABLE, i), tunable_.slack);
    }
    for (uint i = 0; i < num_labels_; ++i) {
      ps_->VirtualUpdate(RowKey(WEIGHT_TABLE, i));
    }
    ps_->VirtualIterate();
  }

  void refresh_weights() {
    for (uint i = 0; i < num_labels_; ++i) {
      ps_->Read(&w_cache_[i], RowKey(WEIGHT_TABLE, i), tunable_.slack);
    }
  }

  void change_weights() {
    float total_change = 0;
    for (uint i = 0; i < num_labels_; ++i) {
      RowData& w_delta_i = w_delta_[i];
      for (uint j = 0; j < feature_dim_; ++j) {
        // CHECK_EQ(w_delta_i[j], w_delta_i[j]) << "nan detected.";
        total_change += w_delta_i.data[j];
      }
      ps_->Update(RowKey(WEIGHT_TABLE, i), &w_delta_i);
    }
    // cout << "total_change = " << total_change << endl;

    // Zero delta.
    for (uint i = 0; i < num_labels_; ++i) {
      RowData& w_delta_i = w_delta_[i];
      for (uint j = 0; j < feature_dim_; j++) {
        w_delta_i.data[j] = 0;
      }
    }
  }

  uint ZeroOneLoss(const vector<float>& prediction, uint label) {
    uint max_idx = 0;
    float max_val = prediction[0];
    for (uint i = 1; i < num_labels_; ++i) {
      if (prediction[i] > max_val) {
        max_val = prediction[i];
        max_idx = i;
      }
    }
    return (max_idx == label) ? 0 : 1;
  }

  float CrossEntropyLoss(const vector<float>& prediction, uint label) {
    // CHECK_LE(prediction[label], 1);
    return SafeLog(prediction[label]);
  }

  vector<float> Predict(RowData& feature) {
    vector<float> y_vec(num_labels_);

    for (uint i = 0; i < num_labels_; ++i) {
      y_vec[i] =
        DenseDenseFeatureDotProduct(feature, w_cache_[i], feature_dim_);
    }

    Softmax(&y_vec);
    return y_vec;
  }

  void SingleDataSGD(RowData& feature, uint label, float learning_rate) {
    vector<float> y_vec = Predict(feature);
    y_vec[label] -= 1.; // See Bishop PRML (2006) Eq. (4.109)

    // outer product
    for (uint i = 0; i < num_labels_; ++i) {
      // w_cache_[i] += -\eta * y_vec[i] * feature
      FeatureScaleAndAdd(
        -learning_rate * y_vec[i], feature, w_cache_[i], feature_dim_);
      FeatureScaleAndAdd(
        -learning_rate * y_vec[i], feature, w_delta_[i], feature_dim_);
    }
  }

  void iteration() {
    refresh_weights();

    float curr_learning_rate = learning_rate_ * pow(decay_rate_, cur_clock_);
    for (uint i = batch_offset_; i < batch_offset_ + batch_size_; i++) {
      SingleDataSGD(train_features_[i], train_labels_[i], curr_learning_rate);
    }

    change_weights();

    /* TODO: report the loss here */
    ps_->Iterate(0.0);
    cur_clock_++;
  }

 public:
  MLRWorker(
      shared_ptr<IterStore> ps,
      int worker_id, int num_workers,
      Tunable tunable,
      int random_seed,
      int num_iterations,
      string format, string data_file,
      double learning_rate, double decay_rate)
      : ps_(ps),
        worker_id_(worker_id), num_workers_(num_workers),
        tunable_(tunable),
        random_seed_(random_seed),
        num_iterations_(num_iterations),
        format_(format), data_file_(data_file),
        learning_rate_(learning_rate), decay_rate_(decay_rate)
  {}

  void operator()() {
    /* Register worker thread to the parameter server */
    ps_->ThreadStart();

    /* Read input data */
    read_data(data_file_);

    /* Report the access sequence with a "virtual iteration" */
    virtual_iteration();

    /* Initialize each parameter data values */
    initialize();

    /* Start training */
    tbb::tick_count start_tick = tbb::tick_count::now();
    for (int i = 0; i < num_iterations_; i++) {
      iteration();
    }
    double training_time = (tbb::tick_count::now() - start_tick).seconds();

    /* Report stats */
    string json_stats;
    if (worker_id_ == 0) {
      json_stats = ps_->GetStats();
      cout << "Training time = "
           << training_time
           << " seconds"
           << endl;
      cout << "STATS: { "
           << json_stats
           << " }" << endl;
    }

    ps_->ThreadStop();
  }
};

#endif /* MLR_HPP_ */
