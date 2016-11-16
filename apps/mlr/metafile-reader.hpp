// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.10.18

#ifndef METAFILE_READER_HPP_
#define METAFILE_READER_HPP_

#include <string>
#include <tr1/unordered_map>
#include <fstream>
#include <sstream>

using std::tr1::unordered_map;

// Read meta file consisting of "name: value" on each line.
class MetafileReader {
public:
  MetafileReader();
  MetafileReader(const std::string& metafile_path);
  void Init(const std::string& metafile_path);

  int get_int32(std::string key);
  double get_double(std::string key);
  bool get_bool(std::string key);
  std::string get_string(std::string key);

private:
  unordered_map<std::string, std::string> content_;

  std::string metafile_path_;

};

MetafileReader::MetafileReader() { }

MetafileReader::MetafileReader(const std::string& metafile_path) {
  Init(metafile_path);
}

void MetafileReader::Init(const std::string& metafile_path) {
  metafile_path_ = metafile_path;
  std::ifstream is(metafile_path_.c_str());
  assert(is);
  // CHECK(is) << "Failed to open " << metafile_path_;
  std::string line;
  while (std::getline(is, line)) {
    std::stringstream linestream(line);
    std::string field_name;
    std::string field_val;
    linestream >> field_name >> field_val;
    // CHECK_EQ(":", field_name.substr(field_name.size() - 1))
      // << "Field name needs to be 'field_name: field_value' ";
    field_name = field_name.substr(0, field_name.size() - 1);
    content_[field_name] = field_val;
  }
  is.close();
}

// -------------------- Getters ----------------------

int MetafileReader::get_int32(std::string key) {
  return atoi(get_string(key).c_str());
}

double MetafileReader::get_double(std::string key) {
  return atof(get_string(key).c_str());
}

bool MetafileReader::get_bool(std::string key) {
  return get_string(key).compare("1") == 0;
}

std::string MetafileReader::get_string(std::string key) {
  unordered_map<std::string, std::string>::iterator it = content_.find(key);
  // CHECK(it != content_.end())
    // << "Failed to lookup " << key << " in " << metafile_path_;
  return it->second;
}

#endif    /* METAFILE_READER_HPP_ */
