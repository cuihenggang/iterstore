/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <assert.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>

using std::string;
using std::cout;
using std::endl;
using std::istringstream;
using std::ifstream;
using std::ofstream;
using std::vector;
using std::map;

struct Edge {
  int src;  /* Source node ID */
  int dst;  /* Destination node ID */
  Edge(int src_i, int dst_i)
    : src(src_i), dst(dst_i) {}
  Edge() {}
};

void convert_data(char *inputfile, char *outputfile) {
  string line;
  ifstream inputstream(inputfile);
  ofstream outputstream(outputfile);

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
        cout << "total_nodes = " << total_nodes
             << ", total_edges = " << total_edges
             << endl;
      }
    }
    outputstream << line << endl;
  }

  /* Read edges */
  vector<Edge> edges(total_edges);
  map<int, int> node_id_map;
  for (int i = 0; i < total_edges; i++) {
    getline(inputstream, line);
    /* Read edge */
    istringstream linestream(line);
    int src, dst;
    linestream >> src >> dst;
    edges[i] = Edge(src, dst);
    node_id_map[src] = -1;
    node_id_map[dst] = -1;
  }
  inputstream.close();

  int node_id = 0;
  for (map<int, int>::iterator it = node_id_map.begin();
       it != node_id_map.end(); it++) {
    it->second = node_id++;
  }
  assert(node_id == total_nodes);

  /* Write edges */
  for (int i = 0; i < total_edges; i++) {
    outputstream << node_id_map[edges[i].src]
                 << " " << node_id_map[edges[i].dst]
                 << endl;
  }
  outputstream.close();
}

int main(int argc, char* argv[]) {
  convert_data(argv[1], argv[2]);
}
