/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <boost/program_options.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>

#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <map>
#include <vector>
#include <utility>

// The serialization for unordered_map is provided by the following hack
#include "server/boost_serialization_unordered_map.hpp"
#include "include/lazy-table-module-types.hpp"

typedef boost::unordered_map<TableRow, RowData> Tables;

using std::string;
using std::cout;
using std::endl;
using std::pair;
using std::ifstream;
using std::vector;
using std::map;
using std::istringstream;
using std::make_pair;

namespace po = boost::program_options;

uint32_t N_, V_, nShards_, K_;
double alpha_, lambda_;
std::vector<Tables> shards;

typedef pair<uint32_t, float> walltime_t;

void read_snapshot(std::string path, uint32_t iter) {
  shards.clear();
  shards.resize(nShards_);

  for (uint32_t i = 0; i < nShards_; ++i) {
    std::string filepath = path + "/snapshot."
                           + boost::lexical_cast<std::string>(iter) + "."
                           + boost::lexical_cast<std::string>(i);
    // std::cout << "...Reading " << filepath << std::endl;
    std::ifstream fin(filepath.c_str());
    boost::archive::text_iarchive ia(fin);
    ia >> shards[i];
    fin.close();
  }
}

typedef std::pair<TableRow, RowData> KeyAndRow;
typedef std::pair<col_idx_t, val_t> ColPair;
void print_shards() {
  for (uint32_t i = 0; i < shards.size(); ++i) {
    std::cout << "Shard " << i << std::endl;
    BOOST_FOREACH(KeyAndRow keyRow, shards[i]) {
      std::cout << "...Table id " << keyRow.first.first
                << " row index " << keyRow.first.second << std::endl;
#if defined(VECTOR_ROW)
      uint col_idx = 0;
      BOOST_FOREACH(val_t value, keyRow.second) {
        std::cout << col_idx++ << ":" << value << " ";
      }
#else
      BOOST_FOREACH(ColPair index_value, keyRow.second) {
        std::cout << index_value.first << ":" << index_value.second << " ";
      }
#endif
      std::cout << std::endl;
    }
  }
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
  string datafile, vocabfile, snaplist;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("snaplist",
     po::value<string>(&snaplist),
     "List of snapshot paths")
    ("nShards",
     po::value<uint32_t>(&nShards_),
     "Number of shards");
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
       << ", K = " << K_
       << ", nShards = " << nShards_
       << endl;

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
      read_snapshot(snap, iter);
      print_shards();


      // JSON output
      std::cout << "{"
                << " \"time\": " << time
                // << ", \"data_file\": \"" << datafile << "\""
                // << ", \"quality\": " << ll
                << ", \"nr_iteration\": " << iter
                << ", \"snap_dir\": \"" << snap << "\""
                << " }" << endl;
    }
  }
}
