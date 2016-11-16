/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <boost/program_options.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/unordered_map.hpp>

#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>

// The serialization for unordered_map is provided by the following hack
#include "server/boost_serialization_unordered_map.hpp"
#include "include/lazy-table-module-types.hpp"
#include "server/tablet-server.hpp"

// The storage is a mix of rows from different tables, index by table/row pair
typedef boost::unordered_map<TableRow, TabletStorage::RowStorage> Tables;
Tables shardsMerged;

#include "random.hpp"

using std::string;
using std::vector;
using std::pair;
using std::ifstream;
using std::istringstream;
using std::make_pair;
using std::cout;
using std::endl;

uint32_t N_, V_, nShards_, num_channels;
double alpha_, lambda_;

typedef std::pair<uint32_t, float> walltime_t;

namespace po = boost::program_options;

void read_snapshot(std::string path, uint32_t iter) {
  shardsMerged.clear();
  uint total_rows = 0;

  for (uint32_t i = 0; i < nShards_; ++i) {
    for (uint32_t j = 0; j < num_channels; ++j) {
      TabletStorage::TableStorage shard;
      std::string filepath = path + "/snapshot."
                           + boost::lexical_cast<std::string>(iter) + "."
                           + boost::lexical_cast<std::string>(i) + "."
                           + boost::lexical_cast<std::string>(j);
      std::ifstream fin(filepath.c_str());
      // boost::archive::text_iarchive ia(fin);
      // ia >> shard;
      uint count;
      fin.read(reinterpret_cast<char *>(&count), sizeof(count));
      shard.resize(count);
      fin.read(
          reinterpret_cast<char *>(shard.data()),
          sizeof(TabletStorage::RowStorage) * count);
      total_rows += shard.size();
      fin.close();

      // insert all rows into the map that's the joint set of all shards
      for (uint k = 0; k < shard.size(); k++) {
        TableRow table_row(shard[k].table, shard[k].row);
        shardsMerged[table_row] = shard[k];
      }
    }
  }
  assert(total_rows == shardsMerged.size());
}

typedef std::pair<TableRow, TabletStorage::RowStorage> KeyAndRow;
void print_shards() {
  BOOST_FOREACH(KeyAndRow keyRow, shardsMerged) {
    std::cout << "...Table id " << keyRow.first.first
              << " row index " << keyRow.first.second << std::endl;
    for (uint col_idx = 0; col_idx < ROW_DATA_SIZE; col_idx++) {
      std::cout << col_idx << ":" << keyRow.second.data.data[col_idx] << " ";
    }
    std::cout << std::endl;
  }
}

// Read N, V from LDA datafiles
void read_lda_data(std::string inputfile, std::string vocabfile,
                   bool zero_indexed = true) {
  std::string line;

  // Count number of words from vocabulary file
  std::ifstream vocabstream(vocabfile.c_str());
  V_ = 0;
  while (!!getline(vocabstream, line)) {
    ++V_;
  }
  vocabstream.close();

  // Count number of documents from LDA data file
  std::ifstream inputstream(inputfile.c_str());
  N_ = 0;
  while (!!getline(inputstream, line)) {
    ++N_;
  }
  inputstream.close();
}

void read_walltimefile(string inputfile, vector<walltime_t>& wtvec) {
  string line;
  ifstream inputstream(inputfile.c_str());

  wtvec.clear();
  while (!!getline(inputstream, line)) {
    uint32_t iter;
    float time;
    istringstream iss(line);
    iss >> iter >> time;
    wtvec.push_back(make_pair(iter, time));
  }

  inputstream.close();
}

// RowData here is ArrayData with -DARRAY_ROW
RowData* read_sthetarow(uint32_t i) {
  TableRow table_row(0, i);
  if (shardsMerged.find(table_row) == shardsMerged.end()) {
    return NULL;
  }
  return &(shardsMerged.find(table_row)->second.data);
}

// RowData here is ArrayData with -DARRAY_ROW
RowData *read_sbetarow(uint32_t i) {
  TableRow table_row(1, i);
  if (shardsMerged.find(table_row) == shardsMerged.end()) {
    return NULL;
  }
  return &(shardsMerged.find(table_row)->second.data);
}

// Computes the complete log-likelihood using the sufficient statistics
// (i.e. LazyTables stheta_, sbeta_).
double complete_ll() {
  double ll = 0.0;
  uint32_t total_words = 0;

  // Contribution from P(z)
  for (uint32_t i = 0; i < N_; ++i) {  // "external" doc indices
    ll += Random::lnGamma(ROW_DATA_SIZE*alpha_);
    uint32_t total_tokens = 0;
    RowData* this_theta_ptr = read_sthetarow(i);
    if (this_theta_ptr != NULL) {
      for (uint32_t k = 0; k < ROW_DATA_SIZE; ++k) {
        int64_t k_token_count = this_theta_ptr->data[k];
        if (k_token_count > 0) {
          total_tokens += k_token_count;
          ll += Random::lnGamma(k_token_count + alpha_) -
            Random::lnGamma(alpha_);
        } else if (k_token_count < 0) {
          std::cerr << "complete_ll(): detected negative entry stheta_["
                    << i << "][" << k << "] = " << this_theta_ptr->data[k]
                    << ", skipping...\n";
        }
      }
    }
    ll -= Random::lnGamma(ROW_DATA_SIZE*alpha_ + total_tokens);
    total_words += total_tokens;
  }

  // Contribution from P(w|z)
  total_words = 0;
  RowData *sum_beta_ptr = read_sbetarow(V_);
  for (uint32_t k = 0; k < ROW_DATA_SIZE; ++k) {
    ll += Random::lnGamma(V_*lambda_) -
      Random::lnGamma(V_*lambda_ + sum_beta_ptr->data[k]);
    if (sum_beta_ptr->data[k] < 0) {
      std::cerr << "complete_ll(): sum_beta[" << k << "] = "
                << sum_beta_ptr->data[k]
                << " was negative, I couldn't do anything...\n";
    }
    total_words += sum_beta_ptr->data[k];
  }

  for (uint32_t v = 0; v < V_; ++v) {
    RowData *this_beta_ptr = read_sbetarow(v);
    if (this_beta_ptr != NULL) {
      for (uint32_t k = 0; k < ROW_DATA_SIZE; ++k) {
        int64_t v_count = this_beta_ptr->data[k];
        if (v_count > 0) {
          ll += Random::lnGamma(v_count + lambda_) -
            Random::lnGamma(lambda_);
        } else if (v_count < 0) {
          std::cerr << "complete_ll(): detected negative entry sbeta_["
                    << v << "][" << k << "] = " << this_beta_ptr->data[k]
                    << ", skipping...\n";
        }
      }
    }
  }

  return ll;
}

int main(int argc, char* argv[]) {
  string datafile, vocabfile, snaplist;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("data",
     po::value<string>(&datafile),
     "LDA data file")
    ("vocab",
     po::value<string>(&vocabfile),
     "LDA vocab file")
    ("snaplist",
     po::value<string>(&snaplist),
     "List of snapshot paths")
    ("nShards",
     po::value<uint32_t>(&nShards_),
     "Number of shards")
    ("alpha",
     po::value<double>(&alpha_)->default_value(0.1),
     "Hyperparameter alpha")
    ("lambda",
     po::value<double>(&lambda_)->default_value(0.1),
     "Hyperparameter lambda")
    ("num_channels",
     po::value<uint32_t>(&num_channels)->default_value(1),
     "Number of channels");
  po::variables_map options_map;
  po::store(po::parse_command_line(argc, argv, desc), options_map);
  po::notify(options_map);

  // Parse snaplist
  vector<string> snapvec;
  istringstream iss(snaplist);
  string temp;
  while (true) {
    iss >> temp;
    if (!iss) {
      break;
    }
    snapvec.push_back(temp);
  }

  // Print options
  cout << "data = " << datafile
       << ", vocab = " << vocabfile
       << ", ROW_DATA_SIZE = " << ROW_DATA_SIZE
       << ", nShards = " << nShards_
       << endl;

  read_lda_data(datafile, vocabfile);

  // Go through each snapshot directory
  BOOST_FOREACH(string snap, snapvec) {
    // Read walltime file (shard 0) and get iterations
    // cout << "Reading snapshots at " << snap << endl;
    vector<walltime_t> wtvec;
    read_walltimefile(snap + "/walltime.0.0", wtvec);

    cout << "From " << snap << endl;

    BOOST_FOREACH(walltime_t wt, wtvec) {
      uint32_t iter = wt.first;
      float time = wt.second;

      // Read snapshot
      read_snapshot(snap, iter);
      // print_shards();

      // Compute log-likelihood
      double ll = complete_ll();
      // cout << "...Log-likelihood at iter " << iter << " = " << ll << endl;

      // JSON output
      std::cout << "{"
                << " \"time\": " << time
                << ", \"data_file\": \"" << datafile << "\""
                << ", \"quality\": " << ll
                << ", \"nr_iteration\": " << iter
                << ", \"snap_dir\": \"" << snap << "\""
                << " }" << endl;
    }
  }
}
