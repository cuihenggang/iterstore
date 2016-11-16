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

#include "mlr-util.hpp"
#include "metafile-reader.hpp"

namespace po = boost::program_options;

using std::string;
using std::vector;
using std::pair;
using std::ifstream;
using std::istringstream;
using std::make_pair;
using std::cout;
using std::endl;
using std::map;

typedef std::pair<uint, float> walltime_t;
typedef pair<uint, float> uint32_pair_double;
typedef uint uint;

// The storage is a mix of rows from different tables, index by table/row pair
typedef boost::unordered_map<TableRow, TabletStorage::RowStorage> Tables;
Tables shardsMerged;

uint N_, M_, nnz_, nShards_, num_channels;
vector<RowData> train_features_;
vector<int> train_labels_;
vector<RowData> w_cache_;

uint num_train_data_;
uint feature_dim_;
uint num_labels_;
string read_format_;
bool feature_one_based_;
bool label_one_based_;
bool snappy_compressed_;

void read_snapshot(std::string path, uint iter) {
  shardsMerged.clear();
  uint total_rows = 0;

  std::cerr << "nShards_ = " << nShards_
            << " num_channnels = " << num_channels << std::endl;

  for (uint i = 0; i < nShards_; ++i) {
    for (uint j = 0; j < num_channels; ++j) {
      TabletStorage::TableStorage shard;
      std::string filepath = path + "/snapshot."
                           + boost::lexical_cast<std::string>(iter) + "."
                           + boost::lexical_cast<std::string>(i) + "."
                           + boost::lexical_cast<std::string>(j);
      //std::cerr << "read from " << filepath << std::endl;
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
  cerr << "total_rows = " << total_rows << std::endl;
  cerr << "size =" << shardsMerged.size() << std::endl;
  assert(total_rows == shardsMerged.size());
}

typedef std::pair<TableRow, TabletStorage::RowStorage> KeyAndRow;
void print_shards() {
  BOOST_FOREACH(KeyAndRow keyRow, shardsMerged) {
    std::cerr << "...Table id " << keyRow.first.first
              << " row index " << keyRow.first.second << std::endl;
    for (uint col_idx = 0; col_idx < ROW_DATA_SIZE; col_idx++) {
      std::cerr << col_idx << ":" << keyRow.second.data.data[col_idx] << " ";
    }
    std::cerr << std::endl;
  }
}

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

void read_data(string datafile_prefix) {
  read_metafile(datafile_prefix);
  string train_file = datafile_prefix;
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
}

void read_walltimefile(string inputfile, vector<walltime_t>& wtvec) {
  string line;
  ifstream inputstream(inputfile.c_str());

  wtvec.clear();
  while (!!getline(inputstream, line)) {
    uint iter;
    float time;
    istringstream iss(line);
    iss >> iter >> time;
    wtvec.push_back(make_pair(iter, time));
  }

  inputstream.close();
}

RowData& read_w_row(uint i) {
  TableRow table_row(0, i);
  return shardsMerged[table_row].data;
}

void read_w_table() {
  w_cache_.resize(num_labels_);
  for (uint i = 0; i < num_labels_; i++) {
    w_cache_[i] = read_w_row(i);
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

float calc_obj() {
  float zero_one_loss = 0.0;
  float entropy_loss = 0.0;
  for (uint i = 0; i < train_features_.size(); ++i) {
    vector<float> pred =
        Predict(train_features_[i]);
      zero_one_loss += ZeroOneLoss(pred, train_labels_[i]);
      entropy_loss += CrossEntropyLoss(pred, train_labels_[i]);
  }
  zero_one_loss /= train_features_.size();
  entropy_loss /= train_features_.size();
  return zero_one_loss;
  // return entropy_loss;
}

int main(int argc, char* argv[]) {
  string datafile, snaplist;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("data",
     po::value<string>(&datafile),
     "Data file")
    ("snaplist",
     po::value<string>(&snaplist),
     "List of snapshot paths")
    ("nShards",
     po::value<uint>(&nShards_),
     "Number of shards")
      ("num_channels",
       po::value<uint>(&num_channels)->default_value(1),
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
  cerr << "data = " << datafile
       << ", ROW_DATA_SIZE = " << ROW_DATA_SIZE
       << ", nShards = " << nShards_
       << endl;
  cerr << "Paths to compute:" << endl;
  BOOST_FOREACH(string snap, snapvec) {
    cerr << snap << endl;
  }

  // Read data
  std::cerr << "Reading datafile " << datafile << endl;
  read_data(datafile);
  std::cerr << "...Nonzeros = " << nnz_ << endl;

    // Go through each snapshot directory
  BOOST_FOREACH(string snap, snapvec) {
    // Read walltime file (shard 0) and get iterations
    cerr << "Reading snapshots at " << snap << endl;

    vector<walltime_t> wtvec;
    read_walltimefile(snap + "/walltime.0.0", wtvec);

    cout << "From " << snap << endl;
    BOOST_FOREACH(walltime_t wt, wtvec) {
      uint iter = wt.first;
      float time = wt.second;

      cerr << " iter = " << iter
           << " time = " << time << std::endl;

    read_snapshot(snap, iter);
    read_w_table();
    //print_shards();

    // Compute objective
    float obj = calc_obj();
    std::cerr << "...Objective at iter " << iter << " = " << obj << endl;

    // JSON output
    std::cout << "{"
              << " \"time\": " << time
              << ", \"data_file\": \"" << datafile << "\""
              << ", \"quality\": " << obj
              << ", \"nr_iteration\": " << iter
              << ", \"snap_dir\": \"" << snap << "\""
              << " }" << endl;
    }

    return 0;
  }
}
