#ifndef __PAGERANK_HPP__
#define __PAGERANK_HPP__

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
#include <limits>
#include <fstream>

#include <boost/unordered_set.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/format.hpp>

#include <glog/logging.h>

#include <tbb/tick_count.h>

#include "iterstore.hpp"

#define RANK_TABLE    0

#define D 0.85

using std::string;
using std::cout;
using std::cerr;
using std::endl;
using std::istringstream;
using boost::shared_ptr;

typedef std::vector<int> uint32Vec;
typedef std::vector<double> doubleVec;

/* In the PageRank application, we store the rank of each node as a row,
 * so we should set ROW_DATA_SIZE to 1 in user-defined-types.hpp */
class PageRankWorker {
  struct Edge {
    int src;  /* Source node ID */
    int dst;  /* Destination node ID */
    Edge() {}
    Edge(int src, int dst)
      : src(src), dst(dst) {}
  };

  shared_ptr<IterStore> ps_;
  int worker_id_;
  int num_workers_;
  Tunable tunable_;
  int random_seed_;
  int num_iterations_;
  std::string format_;
  std::string data_file_;

  std::vector<Edge> edges_;
  std::vector<float> rank_contribs_;
  std::vector<int> src_degrees_;
  boost::unordered_set<int> src_node_set_;
  boost::unordered_set<int> dst_node_set_;

  void read_partbin(std::string inputfile_prefix) {
    std::string inputfile =
      (boost::format("%s.partbin/%i") % inputfile_prefix % num_workers_).str();
    std::ifstream inputstream(inputfile.c_str(), std::ios::binary);
    CHECK(inputstream);

    int num_edges;
    inputstream.read(reinterpret_cast<char *>(&num_edges),
                     sizeof(int));
    edges_.resize(num_edges);
    src_degrees_.resize(num_edges);
    rank_contribs_.resize(num_edges);
    inputstream.read(reinterpret_cast<char *>(edges_.data()),
                     sizeof(Edge) * num_edges);
    inputstream.close();
  }

  /* Read data from SNAP format (this SNAP has nothing to do with snapshots */
  void read_snap_data(std::string inputfile) {
    std::string line;
    std::ifstream inputstream(inputfile.c_str());
    CHECK(inputstream);

    int total_nodes = 0;
    int total_edges = 0;

    /* Parse graph size from comment strings */
    while (inputstream.peek() == static_cast<int>('#')) {
      getline(inputstream, line);
      istringstream linestream(line);
      string tmp_str;
      linestream >> tmp_str;
      if (!linestream.eof()) {
        linestream >> tmp_str;
        if (tmp_str.compare("Nodes:") == 0) {
          linestream >> total_nodes >> tmp_str >> total_edges;
        }
      }
    }

    /* Determine how many edges_ this computer instance should get */
    int div = total_edges / worker_id_;
    int res = total_edges % worker_id_;
    int num_edges = div + (res > num_workers_ ? 1 : 0);
    int offset = div * num_workers_ + (res > num_workers_ ? num_workers_ : res);

    /* Read edges_ */
    edges_.resize(num_edges);
    rank_contribs_.resize(num_edges);
    src_degrees_.resize(num_edges);
    uint64_t edge_id = 0;
    for (uint64_t i = 0; i < total_edges; i++) {
      getline(inputstream, line);
      if (i >= offset && edge_id < edges_.size()) {
        /* Read edge */
        std::istringstream linestream(line);
        assert(edge_id < num_edges);
        int src, dst;
        linestream >> src >> dst;
        edges_[edge_id++] = Edge(src, dst);
      }
    }
    assert(edge_id == num_edges);

    inputstream.close();

    std::string outputfile =
        (boost::format("%s.partbin/%i") % inputfile % num_workers_).str();
    std::ofstream outputstream(
        outputfile.c_str(), std::ios::out | std::ios::binary);
    outputstream.write(reinterpret_cast<char *>(&num_edges),
                       sizeof(num_edges));
    outputstream.write(reinterpret_cast<char *>(edges_.data()),
                       sizeof(Edge) * num_edges);
    outputstream.close();
  }

  /* Read data from ADJ format:
   * The Adjacency list file format stores on each line, a source vertex,
   * followed by a list of all target vertices: each line has the following
   * format:
   *   [vertex ID]  [number of target vertices] [target ID 1] [target ID 2] ...
   */
  void read_adj_data(std::string inputfile) {
    std::string line;
    std::ifstream inputstream(inputfile.c_str());
    CHECK(inputstream);

    int total_nodes = 0;
    int total_edges = 0;

    int last_report = 0;

    /* Parse graph size from a first round sweep */
    while (!inputstream.eof()) {
      getline(inputstream, line);
      std::istringstream linestream(line);
      int src_id, num_edges;
      linestream >> src_id >> num_edges;
      total_edges += num_edges;
      if (num_workers_ == 0 && (total_edges - last_report) > 1000000) {
        cout << total_edges << " edges found" << endl;
        last_report = total_edges;
      }
    }
    inputstream.close();
    cout << "total_nodes = " << total_nodes
         << ", total_edges = " << total_edges
         << endl;

    /* Determine how many edges and the edge offset this computing instance
     * should get
     */
    int div = total_edges / worker_id_;
    int res = total_edges % worker_id_;
    int num_edges = div + (res > num_workers_ ? 1 : 0);
    int offset = div * num_workers_ +
                        (res > num_workers_ ? num_workers_ : res);

    /* Second round, read edges */
    inputstream.open(inputfile.c_str());
    edges_.resize(num_edges);
    src_degrees_.resize(num_edges);
    rank_contribs_.resize(num_edges);
    int local_id = 0;
    int total_id = 0;
    while (!inputstream.eof()) {
      getline(inputstream, line);
      std::istringstream linestream(line);
      int src_id, num_edges;
      linestream >> src_id >> num_edges;

      if (local_id < edges_.size() && total_id + num_edges > offset) {
        /* Has edges to be loaded in this line */
        for (int j = 0; j < num_edges; j++) {
          int dst_id;
          linestream >> dst_id;

          total_id++;

          if (local_id < edges_.size() && total_id > offset) {
            edges_[local_id] = Edge(src_id, dst_id);
            local_id++;
            if (local_id == edges_.size()) {
              /* Check data loading */
              if (total_id != offset + edges_.size()) {
                cerr << "num_workers_ = " << num_workers_
                     << " div = " << div
                     << " res = " << res
                     << " offset = " << offset
                     << " total_id = " << total_id
                     << " edges_.size() = " << edges_.size()
                     << endl;
                assert(total_id == offset + edges_.size());
              }
            }
          }
        }
      } else {
        /* total_id is always increased no matter whether data is loaded */
        total_id += num_edges;
      }
    }

    CHECK_EQ(total_id, total_edges);
    inputstream.close();
  }

  void initialize() {
    /* Initilize node_set */
    for (int i = 0; i < edges_.size(); i ++) {
      src_node_set_.insert(edges_[i].src);
      dst_node_set_.insert(edges_[i].dst);
    }

    /* Initialize node degree */
    for (int i = 0; i < edges_.size(); i ++) {
      RowData update_row;
      update_row.data[0] = 1;
      ps_->Update(RowKey(RANK_TABLE, edges_[i].src), &update_row);
    }

    /* Synchronize before reading degrees */
    ps_->Iterate(0.0);

    for (int i = 0; i < edges_.size(); i ++) {
      RowData buffer;
      ps_->Read(&buffer, RowKey(RANK_TABLE, edges_[i].src), 0);
                        /* read with slack 0, guarantee a synchronization */
      src_degrees_[i] = static_cast<int>(buffer.data[0]);
    }

    /* Synchronize before intializing ranks */
    ps_->Iterate(0.0);
    RowData buffer;
    ps_->Read(&buffer, RowKey(RANK_TABLE, edges_[0].src), 0);
        /* Use this read as a barrier */

    /* Initialize node rank (from outbound links) */
    for (int i = 0; i < edges_.size(); i ++) {
      float rank_update = (1.0f - D) / src_degrees_[i] - 1;
      /* The initial value for each node is (1 - d) / N, and we distribute the
       * value to all inbound links.
       * We also need to substract one here because we have already added one
       * for calculating its degree. */
      RowData update_row;
      update_row.data[0] = rank_update;
      ps_->Update(RowKey(RANK_TABLE, edges_[i].src), &update_row);
    }
  }

  void virtual_access_edge(int i) {
    Edge& edge = edges_[i];
    ps_->VirtualRead(RowKey(RANK_TABLE, edge.src), tunable_.slack);
    ps_->VirtualUpdate(RowKey(RANK_TABLE, edge.dst));
  }

  void virtual_iteration() {
    for (int i = 0; i < edges_.size(); i++) {
      virtual_access_edge(i);
    }
    ps_->VirtualIterate();
    ps_->FinishVirtualIteration();
  }

  void compute_edge(int i) {
    Edge& edge = edges_[i];

    /* Read the rank of src */
    RowData src_rank_row;
    ps_->Read(&src_rank_row, RowKey(RANK_TABLE, edge.src), tunable_.slack);
    float src_rank = src_rank_row.data[0];

    /* Update the rank of dst */
    float new_rank_contribution = D * src_rank / src_degrees_[i];
    float rank_update = new_rank_contribution - rank_contribs_[i];
    RowData update_row;
    update_row.data[0] = rank_update;
    ps_->Update(RowKey(RANK_TABLE, edge.dst), &update_row);

    /* Record rank contribution */
    rank_contribs_[i] = new_rank_contribution;
  }

  void iteration() {
    for (int i = 0; i < edges_.size(); i++) {
      compute_edge(i);
    }
  }

 public:
  PageRankWorker(
      shared_ptr<IterStore> ps,
      int worker_id, int num_workers,
      Tunable tunable,
      int random_seed,
      int num_iterations,
      string format, string data_file)
      : ps_(ps),
        worker_id_(worker_id), num_workers_(num_workers),
        tunable_(tunable),
        random_seed_(random_seed),
        num_iterations_(num_iterations),
        format_(format), data_file_(data_file)
  {}

  void operator()() {
    CHECK_EQ(ROW_DATA_SIZE, 1);

    /* Register worker thread to the parameter server */
    ps_->ThreadStart();

    /* Read data */
    if (!format_.compare("partbin")) {
      read_partbin(data_file_);
    } else if (!format_.compare("snap")) {
      read_snap_data(data_file_);
    } else if (!format_.compare("adj")) {
      read_adj_data(data_file_);
    } else {
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

#endif  // defined __PAGERANK_HPP__
