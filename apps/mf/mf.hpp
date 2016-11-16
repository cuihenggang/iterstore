#ifndef __MF_HPP__
#define __MF_HPP__

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
#include <set>
#include <utility>
#include <limits>
#include <fstream>

#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>
#include <boost/random.hpp>
#include <boost/random/normal_distribution.hpp>

#include <glog/logging.h>

#include <tbb/tick_count.h>

#include "iterstore.hpp"

#define LM_TABLE    0
#define RM_TABLE    1
#define LM_AG_TABLE 2

using std::cout;
using std::cerr;
using std::endl;
using std::vector;
using std::string;
using boost::shared_ptr;


/* Distributed rank-K matrix factorization using squared-loss.
 * We partition rows of the NxM data matrix X across workers (assuming N > M),
 * so the L matrix can be updated locally by each worker,
 * and the R matrix will updated in the parameter server. */
class MFWorker {
  struct Element {
    int i;
    int j;
    float val;
  };

  shared_ptr<IterStore> ps_;
  int worker_id_;
  int num_workers_;
  Tunable tunable_;
  int random_seed_;
  int num_iterations_;
  std::string format_;
  std::string data_file_;
  int update_func_;

  std::vector<Element> elements_;
  int total_elements_in_all_workers_;

  /* Input matrix dimensions */
  int N_, M_;

  RowData Li_gradient;
  RowData Rm_update;

  void read_partbin(std::string inputfile_prefix) {
    std::string inputfile =
      (boost::format("%s/%i") % inputfile_prefix % worker_id_).str();
    std::ifstream inputstream(inputfile.c_str(), std::ios::binary);
    CHECK(inputstream);

    inputstream.read(reinterpret_cast<char *>(&N_), sizeof(N_));
    inputstream.read(reinterpret_cast<char *>(&M_), sizeof(M_));
    inputstream.read(reinterpret_cast<char *>(&total_elements_in_all_workers_),
                     sizeof(total_elements_in_all_workers_));
    int input_size;
    inputstream.read(reinterpret_cast<char *>(&input_size), sizeof(input_size));
    elements_.resize(input_size);
    inputstream.read(reinterpret_cast<char *>((elements_.data())),
                     sizeof(Element) * input_size);
    inputstream.close();
  }

  void initialize() {
    typedef std::set<int> IntSet;
    IntSet i_set;
    IntSet j_set;
    for (int i = 0; i < elements_.size(); i ++) {
      i_set.insert(elements_[i].i);
      j_set.insert(elements_[i].j);
      CHECK_LT(elements_[i].i, N_);
      CHECK_LT(elements_[i].j, M_);
    }
    /* Initialize each parameter value with normal distribution */
    int seed = random_seed_ + worker_id_;
    boost::mt19937 rng(seed);
    boost::normal_distribution<> nd(0.0, 0.1);
    boost::variate_generator<boost::mt19937&, boost::normal_distribution<> >
        var_nor(rng, nd);
    for (IntSet::iterator iter = i_set.begin(); iter != i_set.end(); iter++) {
      int i = *iter;
      RowData *Li_ptr = ps_->LocalAccess(RowKey(LM_TABLE, i));
      RowData& Li = *Li_ptr;
      RowData *Li_accumlated_gradients_ptr =
          ps_->LocalAccess(RowKey(LM_AG_TABLE, i));
      RowData& Li_accumlated_gradients = *Li_accumlated_gradients_ptr;
      for (int k = 0; k < ROW_DATA_SIZE; ++k) {
        val_t value = var_nor();
        Li.data[k] = value;
        Li_accumlated_gradients.data[k] = 0.0;
      }
    }
    for (IntSet::iterator iter = j_set.begin(); iter != j_set.end(); iter++) {
      int j = *iter;
      for (int k = 0; k < ROW_DATA_SIZE; ++k) {
        val_t value = var_nor();
        Rm_update.data[k] = value;
      }
      ps_->Update(RowKey(RM_TABLE, j), &Rm_update);
    }

    /* Push updates to the servers */
    ps_->Iterate();
  }

  void virtual_access_element(int i) {
    Element& element = elements_[i];
    ps_->VirtualLocalAccess(RowKey(LM_TABLE, element.i));
    ps_->VirtualLocalAccess(RowKey(LM_AG_TABLE, element.i));
    ps_->VirtualRead(RowKey(RM_TABLE, element.j), tunable_.slack);
    ps_->VirtualUpdate(RowKey(RM_TABLE, element.j));
  }

  void virtual_iteration() {
    for (int i = 0; i < elements_.size(); i++) {
      virtual_access_element(i);
    }
    ps_->VirtualIterate();
    ps_->FinishVirtualIteration();
  }

  void iteration() {
    val_t loss = 0.0;

    int slack = tunable_.slack;
    val_t learning_rate = tunable_.learning_rate;
    val_t lambda = 0.01;
    val_t lambda_for_L = lambda / ((double)total_elements_in_all_workers_ / N_);
    val_t lambda_for_R = lambda / ((double)total_elements_in_all_workers_ / M_);

    for (int element_id = 0; element_id < elements_.size(); element_id++) {
      loss += sgd(element_id, learning_rate, slack, lambda_for_L, lambda_for_R);
    }

    ps_->Iterate(loss);
  }

  /* Perform gradient descent for data point X(i,j).
   * It will update L(i,:) and R(:,j). */
  val_t sgd(int element_id,
      val_t learning_rate, int slack, val_t lambda_for_L, val_t lambda_for_R) {
    Element& element = elements_[element_id];
    int i = element.i;
    int j = element.j;
    val_t x = element.val;

    /* Read L(i,:), L_accumulated_gradients(i,:), and R(:,j) */
    RowData *Li_ptr = ps_->LocalAccess(RowKey(LM_TABLE, i));
    RowData& Li = *Li_ptr;
    RowData *Li_accumlated_gradients_ptr =
        ps_->LocalAccess(RowKey(LM_AG_TABLE, i));
    RowData& Li_accumlated_gradients = *Li_accumlated_gradients_ptr;

    RowData Rj;
    ps_->Read(&Rj, RowKey(RM_TABLE, j), slack);

    /* Compute L(i,:) * R(:,j) */
    val_t LiRj = 0.0;
    for (int k = 0; k < ROW_DATA_SIZE; ++k) {
      LiRj += Li.data[k] * Rj.data[k];
    }

    /* Calculate the gradients based on the loss function at X(i,j).
     * The loss function at X(i,j) is
     *   ( X(i,j) - L(i,:)*R(:,j) ) ^ 2.
     * The gradient w.r.t. L(i,k) is
     *   2 * ( L(i,:) * R(:,j) - X(i,j) ) * R(k,j) + lambda_L * L(i,k).
     * The gradient w.r.t. R(k,j) is
     *   2 * ( L(i,:) * R(:,j) - X(i,j) ) * L(i,k) + lambda_R * R(k,j).
     * The L matrix will be updated locally with AdaGrad.
     * The R matrix will be updated at the parameter server. */
    val_t diff = LiRj - x;
    for (int k = 0; k < ROW_DATA_SIZE; ++k) {
      val_t gradient = 0.0;
      val_t update = 0.0;

      /* Gradient w.r.t. L(i,k) */
      gradient = 2 * diff * Rj.data[k] + lambda_for_L * Li.data[k];
      update = -gradient;
      Li_gradient.data[k] = update;

      /* Gradient w.r.t. R(k,j) */
      gradient = 2 * diff * Li.data[k] + lambda_for_R * Rj.data[k];
      update = -gradient;
      Rm_update.data[k] = update;
    }

    /* Apply L matrix updates locally with AdaGrad */
    for (int k = 0; k < ROW_DATA_SIZE; k++) {
      val_t gradient = Li_gradient.data[k];
      Li_accumlated_gradients.data[k] += gradient * gradient;
      val_t update =
          learning_rate / sqrt(Li_accumlated_gradients.data[k]) * gradient;
      Li.data[k] += update;
    }

    /* Push the R matrix updates to the parameter server */
    ps_->Update(RowKey(RM_TABLE, j), &Rm_update);

    /* Report loss */
    val_t loss = (x - LiRj) * (x - LiRj);
    return loss;
  }

 public:
  MFWorker(
      shared_ptr<IterStore> ps,
      int worker_id, int num_workers,
      Tunable tunable,
      int random_seed,
      int num_iterations,
      string format, string data_file,
      int update_func)
      : ps_(ps),
        worker_id_(worker_id), num_workers_(num_workers),
        tunable_(tunable),
        random_seed_(random_seed),
        num_iterations_(num_iterations),
        format_(format), data_file_(data_file),
        update_func_(update_func)
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

#endif  // defined __MF_HPP__
