#ifndef MLR_UTIL_HPP_
#define MLR_UTIL_HPP_

#include <stdint.h>
#include <boost/format.hpp>

#include <cmath>
#include <sstream>
#include <string>
#include <vector>
#include <utility>
#include <limits>
#include <fstream>

#include <glog/logging.h>

#include "iterstore.hpp"
#include "fastapprox/fastapprox.hpp"

using std::string;
using std::cout;
using std::cerr;
using std::endl;
using std::istringstream;
using boost::shared_ptr;

typedef uint32_t uint;


const float kCutoff = 1e-15;

const int32_t base = 10;

float SafeLog(float x) {
  if (std::abs(x) < kCutoff) {
    x = kCutoff;
  }
  return fastlog(x);
}

float LogSum(float log_a, float log_b) {
  return (log_a < log_b) ? log_b + fastlog(1 + fastexp(log_a - log_b)) :
    log_a + fastlog(1 + fastexp(log_b-log_a));
}

float LogSumVec(const std::vector<float>& logvec) {
	float sum = 0.;
	sum = logvec[0];
	for (uint i = 1; i < logvec.size(); ++i) {
		sum = LogSum(sum, logvec[i]);
	}
	return sum;
}

void Softmax(std::vector<float>* vec) {
  // CHECK_NOTNULL(vec);
  // TODO(wdai): Figure out why this is necessary. Doubt it is.
	for (uint i = 0; i < vec->size(); ++i) {
		if (std::abs((*vec)[i]) < kCutoff) {
			(*vec)[i] = kCutoff;
    }
	}
	double lsum = LogSumVec(*vec);
	for (uint i = 0; i < vec->size(); ++i) {
		(*vec)[i] = fastexp((*vec)[i] - lsum);
		//(*vec)[i] = exp((*vec)[i] - lsum);
    (*vec)[i] = (*vec)[i] > 1 ? 1. : (*vec)[i];
  }
}

float DenseDenseFeatureDotProduct(const vector<float>& v1,
    const vector<float>& v2) {
  // CHECK_EQ(v1.size(), v2.size());
  float sum = 0.;
  for (uint i = 0; i < v1.size(); ++i) {
    sum += v1[i] * v2[i];
  }
  return sum;
}

void FeatureScaleAndAdd(float alpha, const vector<float>& v1,
    vector<float>& v2) {
  for (uint i = 0; i < v1.size(); ++i) {
   v2[i] += alpha * v1[i];
  }
}

// Return the label. feature_one_based = true assumes
// feature index starts from 1. Analogous for label_one_based.
int32_t ParseLibSVMLine(const std::string& line, std::vector<int32_t>* feature_ids,
    std::vector<float>* feature_vals, bool feature_one_based,
    bool label_one_based) {
  feature_ids->clear();
  feature_vals->clear();
  char *ptr = 0, *endptr = 0;
  // Read label.
  int label = strtol(line.data(), &endptr, base);
  label = label_one_based ? label - 1 : label;
  ptr = endptr;

  while (isspace(*ptr) && static_cast<uint>(ptr - line.data()) < line.size()) ++ptr;
  while (static_cast<uint>(ptr - line.data()) < line.size()) {
    // read a feature_id:feature_val pair
    int32_t feature_id = strtol(ptr, &endptr, base);
    if (feature_one_based) {
      --feature_id;
    }
    feature_ids->push_back(feature_id);
    ptr = endptr;
    // CHECK_EQ(':', *ptr);
    ++ptr;

    feature_vals->push_back(strtod(ptr, &endptr));
    ptr = endptr;
    while (isspace(*ptr) && static_cast<uint>(ptr - line.data()) < line.size()) ++ptr;
  }
  return label;
}

// Open 'filename' and snappy decompress it to std::string.
std::string SnappyOpenFileToString(const std::string& filename) {
  std::ifstream file(filename.c_str(), std::ifstream::binary);
  // CHECK(file) << "Can't open " << filename;
  std::string buffer((std::istreambuf_iterator<char>(file)),
      std::istreambuf_iterator<char>());
  std::string uncompressed;
  // CHECK(snappy::Uncompress(buffer.data(), buffer.size(), &uncompressed))
    // << "Cannot snappy decompress buffer of size " << buffer.size()
    // << "; File: " << filename;
  return uncompressed;
}

std::string OpenFileToString(const std::string& filename) {
  std::ifstream file(filename.c_str());
  // CHECK(file) << "Can't open " << filename;
  std::string buffer((std::istreambuf_iterator<char>(file)),
      std::istreambuf_iterator<char>());
  return buffer;
}

void ReadDataLabelBinary(const std::string& filename,
    uint feature_dim, uint num_data,
    vector<vector<float> >* features, vector<int>* labels,
    bool feature_one_based, bool label_one_based) {
  features->resize(num_data);
  labels->resize(num_data);
  std::ifstream is(filename.c_str(), std::ifstream::binary);
  assert(is);
  // // CHECK(is) << "Failed to open " << filename;
  for (uint i = 0; i < num_data; ++i) {
    // Read the label
    is.read(reinterpret_cast<char*>(&(*labels)[i]), sizeof(int));
    if (label_one_based) {
      --(*labels)[i];
    }
    // Read the feature
    (*features)[i].resize(feature_dim);
    is.read(reinterpret_cast<char*>((*features)[i].data()),
        sizeof(float) * feature_dim);
    if (feature_one_based) {
      for (uint j = 0; j < feature_dim; ++j) {
        --(*features)[i][j];
      }
    }
  }
}

void ReadDataLabelBinary2(const std::string& filename,
    uint feature_dim, uint num_data,
    vector<vector<float> >* features, vector<int>* labels,
    bool feature_one_based, bool label_one_based) {
  features->resize(num_data);
  labels->resize(num_data);
  std::ifstream is(filename.c_str(), std::ifstream::binary);
  uint num_output = 10000;
  std::string output_name =
    filename + boost::lexical_cast<string>(num_output) + ".train";
  std::ofstream os(output_name.c_str(), std::ifstream::binary);
  // // CHECK(is) << "Failed to open " << filename;
  for (uint i = 0; i < num_data; ++i) {
    // Read the label
    is.read(reinterpret_cast<char*>(&(*labels)[i]), sizeof(int));
    if (i < num_output) {
      os.write(reinterpret_cast<char*>(&(*labels)[i]), sizeof(int));
    }
    if (label_one_based) {
      --(*labels)[i];
    }
    // Read the feature
    (*features)[i].resize(feature_dim);
    is.read(reinterpret_cast<char*>((*features)[i].data()),
        sizeof(float) * feature_dim);
    if (i < num_output) {
      os.write(reinterpret_cast<char*>((*features)[i].data()),
          sizeof(float) * feature_dim);
    }
    if (feature_one_based) {
      for (uint j = 0; j < feature_dim; ++j) {
        --(*features)[i][j];
      }
    }
  }
}

void ReadDataLabelLibSVM(const std::string& filename,
    uint feature_dim, uint num_data,
    vector<vector<float> >* features, vector<int>* labels,
    bool feature_one_based, bool label_one_based, bool snappy_compressed) {
  features->resize(num_data);
  labels->resize(num_data);
  std::string file_str = snappy_compressed ?
    SnappyOpenFileToString(filename) : OpenFileToString(filename);
  std::istringstream data_stream(file_str);
  vector<int> feature_ids(feature_dim);
  vector<float> feature_vals(feature_dim);
  uint i = 0;
  for (std::string line; std::getline(data_stream, line) && i < num_data;
      ++i) {
    int label = ParseLibSVMLine(line, &feature_ids,
        &feature_vals, feature_one_based, label_one_based);
    (*labels)[i] = label;
    (*features)[i].resize(feature_dim);
    for (uint j = 0; j < feature_ids.size(); ++j) {
      (*features)[i][feature_ids[j]] = feature_vals[j];
    }
  }
  // // CHECK_EQ(num_data, i) << "Request to read " << num_data
    // << " data instances but only " << i << " found in " << filename;
}

float DenseDenseFeatureDotProduct(
    const RowData& v1, const RowData& v2, uint size) {
  float sum = 0.;
  for (uint i = 0; i < size; ++i) {
    sum += v1.data[i] * v2.data[i];
  }
  return sum;
}

void FeatureScaleAndAdd(
    float alpha, const RowData& v1, RowData& v2, uint size) {
  for (uint i = 0; i < size; ++i) {
    v2.data[i] += alpha * v1.data[i];
  }
}

void copy_vector_to_row_data(
    RowData& row_data, const vector<float>& vec_data, uint size) {
  assert(vec_data.size() <= size);
  for (uint i = 0; i < vec_data.size(); i++) {
    row_data.data[i] = vec_data[i];
  }
}

#endif  /* MLR_UTIL_HPP_ */