/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <boost/foreach.hpp>
#include <boost/program_options.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/serialization/vector.hpp>

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <utility>

// The serialization for unordered_map is provided by the following hack
#include "server/boost_serialization_unordered_map.hpp"
#include "include/lazy-table-module-types.hpp"

// The storage is a mix of rows from different tables, index by table/row pair
typedef boost::unordered_map<TableRow, RowData> Tables;

using std::string;
using std::cout;
using std::endl;
using std::ifstream;
using std::pair;
using std::vector;
using std::make_pair;
using std::istringstream;

namespace po = boost::program_options;

#define RANK_IDX        0
#define OUT_DEGREE_IDX  1
#define IN_DEGREE_IDX   2
#define NODE_TABLE_COL_COUNT 3

typedef pair<uint32_t, float> walltime_t;

// The old style (two layers) row-table structure.
// The lazy-table-module use an one-layer Tables (with <table, row> as index)
// but the initial data file uses the two-layer structure
typedef boost::unordered_map<row_idx_t, RowData> Table;
typedef boost::unordered_map<table_id_t, Table> Tables2;

void read_true_tables(std::string filepath, Tables2& true_tables) {
  // std::cout << "...Reading true table " << filepath << std::endl;
  std::ifstream fin(filepath.c_str());
  boost::archive::binary_iarchive ia(fin);
  ia >> true_tables;
  fin.close();
}

void read_test_tables_shards(std::string path,
                             uint32_t nShards, uint32_t iter,
                             vector<Tables>& shards) {
  shards.clear();
  shards.resize(nShards);

  for (uint32_t i = 0; i < nShards; ++i) {
    std::string filepath = path + "/snapshot."
                           + boost::lexical_cast<std::string>(iter) + "."
                           + boost::lexical_cast<std::string>(i);
    // std::cout << "...Reading test table " << filepath << std::endl;
    std::ifstream fin(filepath.c_str());
    boost::archive::binary_iarchive ia(fin);
    ia >> shards[i];
    fin.close();
  }
}

double cal_diff_square(Tables2& true_tables,
                       std::vector<Tables>& test_tables_shards) {
  double diff_square = 0;
  Table& true_table = true_tables[0];  // only look at the first table
  for (Table::iterator row_it = true_table.begin();
       row_it != true_table.end(); row_it++) {
    RowData& true_row = row_it->second;
    TableRow table_row = make_pair(0, row_it->first);
    RowData& test_row =
      test_tables_shards[row_it->first % test_tables_shards.size()][table_row];

    /* Check degree equal */
    if (true_row[OUT_DEGREE_IDX] != test_row[OUT_DEGREE_IDX] ||
        true_row[IN_DEGREE_IDX] != test_row[IN_DEGREE_IDX]) {
      return -1;
    }

    double diff = true_row[RANK_IDX] - test_row[RANK_IDX];
    diff_square += diff * diff;
  }
  return diff_square;
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

int main(int argc, char* argv[]) {
  string true_snap_file, snaplist;
  uint32_t nShards;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("snaplist", po::value<string>(&snaplist),
     "List of snapshot paths")
    ("nShards", po::value<uint32_t>(&nShards),
     "Number of shards")
    ("ground_truth", po::value<string>(&true_snap_file),
     "True snapshot path");
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

  Tables2 true_tables;
  read_true_tables(true_snap_file, true_tables);

  // Go through each snapshot directory
  BOOST_FOREACH(string snap, snapvec) {
    // Read walltime file (shard 0) and get iterations
    // cout << "Reading snapshots at " << snap << endl;
    vector<walltime_t> wtvec;
    read_walltimefile(snap + "/walltime.0", wtvec);

    cout << "From " << snap << endl;

    BOOST_FOREACH(walltime_t wt, wtvec) {
      uint32_t iter = wt.first;
      float time = wt.second;

      // Read snapshot
      vector<Tables> test_tables_shards;
      read_test_tables_shards(snap, nShards, iter, test_tables_shards);
      double diff_square = cal_diff_square(true_tables, test_tables_shards);

      // JSON output
      std::cout << "{"
                << " \"time\": " << time
                << ", \"data_file\": \"" << true_snap_file << "\""
                << ", \"quality\": " << diff_square
                << ", \"nr_iteration\": " << iter
                << ", \"snap_dir\": \"" << snap << "\""
                << " }" << endl;
    }
  }
}
