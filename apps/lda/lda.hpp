#ifndef __LDA_HPP__
#define __LDA_HPP__

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

#include <cmath>
#include <sstream>
#include <string>
#include <vector>
#include <utility>
#include <set>
#include <limits>
#include <fstream>

#include <boost/shared_ptr.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include <glog/logging.h>

#include <tbb/tick_count.h>

#include "iterstore.hpp"
#include "random.hpp"

#define THETA_TABLE    0
#define BETA_TABLE     1

using std::string;
using std::cout;
using std::endl;
using boost::shared_ptr;


/* LDA with Gibbs Sampling */
class LDAWorker {
  shared_ptr<IterStore> ps_;
  int worker_id_;
  int num_workers_;
  Tunable tunable_;
  int random_seed_;
  int num_iterations_;
  string format_;
  string data_file_;

  /* Model hyperparameters */
  double alpha_;             /* MM vector prior */
  double lambda_;            /* Vocabulary prior */

  /* Probability type to compute */
  enum probMode_t {PRIOR, POSTERIOR};

  /* Observed and latent variables */
  vector<int> w_;
  vector<int> w_offsets_;
  vector<int> z_;

  int total_num_docs_;
  int doc_offset_;
  int num_docs_;   /* Number of docs allocated to this worker thread */
  int vocabulary_size_;

  Random rng_;

  vector<double> probs_;

  /* Gibbs samples a new value for z[x]. Requires the following sufficient
   * statistics as input:
   *
   * my_theta - local copy of theta_table[i,:]
   * my_beta - local copy of beta_table[w[x],:]
   * my_betasum - local copy of beta_table[vocabulary_size_,:]
   *   (i.e. the column sums of beta_table) */
  void sample_z(int x, RowData& my_theta, RowData& my_beta,
                RowData& my_betasum, probMode_t probmode) {
    /* Compute unnormalized prior/posterior probabilities
     * for topics 1 through K */
    for (int k = 0; k < ROW_DATA_SIZE; ++k) {
      /* Prior probability */
      probs_[k] = my_theta.data[k] + alpha_;
      /* Posterior probability */
      if (probmode == POSTERIOR) {
        /* Because compute_w_prob(x) reads z_[x] directly, we
         * set (i.e. condition) z[x] = k and call compute_w_prob(x)
         * to calculate the posterior term. */
        z_[x] = k;
        probs_[k] *= compute_w_prob(x, my_beta, my_betasum);
      }
      CHECK_GE(probs_[k], 0);
    }

    /* Sample a new value for z[x] */
    z_[x] = static_cast<int>(rng_.randDiscrete(probs_, 0, ROW_DATA_SIZE));
  }
  /* Computes P(w|z,beta) for one token */
  double compute_w_prob(int x, RowData& my_beta, RowData& my_betasum) {
    int topic = z_[x];
    return (my_beta.data[topic] + lambda_) /
      (my_betasum.data[topic] + vocabulary_size_*lambda_);
  }

  void read_partbin(string inputfile_prefix) {
    string inputfile =
      (boost::format("%s/%i") % inputfile_prefix % worker_id_).str();
    std::ifstream inputstream(inputfile.c_str(), std::ios::binary);
    CHECK(inputstream) << "inputfile = " << inputfile;
    int w_size;
    inputstream.read(reinterpret_cast<char *>(&total_num_docs_), sizeof(total_num_docs_));
    inputstream.read(reinterpret_cast<char *>(&vocabulary_size_), sizeof(vocabulary_size_));
    inputstream.read(reinterpret_cast<char *>(&doc_offset_),
                     sizeof(doc_offset_));
    inputstream.read(reinterpret_cast<char *>(&num_docs_),
                     sizeof(num_docs_));
    inputstream.read(reinterpret_cast<char *>(&w_size), sizeof(w_size));
    w_.resize(w_size);
    w_offsets_.resize(num_docs_+1);
    inputstream.read(reinterpret_cast<char *>((w_.data())),
                     sizeof(int) * w_size);
    inputstream.read(reinterpret_cast<char *>((w_offsets_.data())),
                     sizeof(int) * (num_docs_+1));
    z_.resize(w_offsets_[num_docs_]);
    inputstream.close();
  }

  void initialize() {
    /* Initialize z's, sufficient statistics */
    for (int i = 0; i < num_docs_; ++i) {
      for (int x = w_offsets_[i]; x < w_offsets_[i+1]; ++x) {
        z_[x] = static_cast<int>(rng_.rand_r() * ROW_DATA_SIZE);

        int global_doc_id = i + doc_offset_;

        RowData update;
        update.data[z_[x]] = 1;
        ps_->Update(RowKey(THETA_TABLE, global_doc_id), &update);
        ps_->Update(RowKey(BETA_TABLE, w_[x]), &update);
        ps_->Update(RowKey(BETA_TABLE, vocabulary_size_), &update);
      }
    }

    /* Push updates to the servers */
    ps_->Iterate();
  }

  void virtual_access_doc(int doc_id) {
    int global_doc_id  = doc_id + doc_offset_;
    for (int x = w_offsets_[doc_id]; x < w_offsets_[doc_id+1]; x++) {
      int w = w_[x];
      ps_->VirtualRead(RowKey(THETA_TABLE, global_doc_id), tunable_.slack);
      ps_->VirtualRead(RowKey(BETA_TABLE, w), tunable_.slack);
      ps_->VirtualRead(RowKey(BETA_TABLE, vocabulary_size_), tunable_.slack);
      ps_->VirtualUpdate(RowKey(THETA_TABLE, global_doc_id));
      ps_->VirtualUpdate(RowKey(BETA_TABLE, w));
      ps_->VirtualUpdate(RowKey(BETA_TABLE, vocabulary_size_));
    }
  }

  void virtual_iteration() {
    for (int i = 0; i < num_docs_; i++) {
      virtual_access_doc(i);
    }
    ps_->VirtualIterate();
    ps_->FinishVirtualIteration();
  }

  void process_doc(int doc_id) {
    int global_doc_id  = doc_id + doc_offset_;
    for (int x = w_offsets_[doc_id]; x < w_offsets_[doc_id+1]; ++x) {
      /* Sample word x in doc doc_id */
      int w = w_[x];
      RowData my_theta;
      RowData my_beta;
      RowData my_betasum;
      ps_->Read(&my_theta, RowKey(THETA_TABLE, global_doc_id), tunable_.slack);
      ps_->Read(&my_beta, RowKey(BETA_TABLE, w), tunable_.slack);
      ps_->Read(&my_betasum, RowKey(BETA_TABLE, vocabulary_size_), tunable_.slack);

      RowData update;
      int z_old = z_[x];
      CHECK_GT(my_beta.data[z_old], 0);
      /* Sample a new value for z[x] */
      sample_z(x, my_theta, my_beta, my_betasum, POSTERIOR);
      /* Add (new) z[x] and w[x] to sufficient stats */
      int z_new = z_[x];
      if (z_old != z_new) {
        update.data[z_old] = -1;
        update.data[z_new] = 1;
      }
      ps_->Update(RowKey(THETA_TABLE, global_doc_id), &update);
      ps_->Update(RowKey(BETA_TABLE, w), &update);
      ps_->Update(RowKey(BETA_TABLE, vocabulary_size_), &update);
    }
  }

  void iteration() {
    for (int doc_id = 0; doc_id < num_docs_; doc_id++) {
      process_doc(doc_id);
    }
    ps_->Iterate(0.0);
  }

 public:
  LDAWorker(
      shared_ptr<IterStore> ps,
      int worker_id, int num_workers,
      Tunable tunable,
      int random_seed,
      int num_iterations,
      string format, string data_file,
      double alpha, double lambda)
      : ps_(ps),
        worker_id_(worker_id), num_workers_(num_workers),
        tunable_(tunable),
        random_seed_(random_seed),
        num_iterations_(num_iterations),
        format_(format), data_file_(data_file),
        alpha_(alpha), lambda_(lambda),
        rng_(random_seed),
        probs_(ROW_DATA_SIZE)
  {}

  void operator()() {
    /* Register worker thread to the parameter server */
    ps_->ThreadStart();

    /* Read input data */
    if (format_ == "partbin") {
      read_partbin(data_file_);
    } else {
      cerr << "unknown data file format" << endl;
      CHECK(0);
    }

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

#endif
