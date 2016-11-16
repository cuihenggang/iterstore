#ifndef SHARED_RANDOM_HPP__
#define SHARED_RANDOM_HPP__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <boost/unordered_set.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/gamma_distribution.hpp>
#include <boost/random/exponential_distribution.hpp>
#include <boost/random/variate_generator.hpp>

#include <vector>
#include <utility>
#include <limits>
#include <cmath>
#include <algorithm>

// Random number generator
class Random {
  boost::mt19937 generator;
  boost::uniform_real<> zero_one_dist;
  boost::variate_generator<boost::mt19937&,
        boost::uniform_real<> > zero_one_generator;

 public:
  explicit Random(unsigned int seed) :
    generator(seed),
    zero_one_dist(0, 1),
    zero_one_generator(generator, zero_one_dist)
  { }

  // Draws a random unsigned integer
  unsigned int randInt() {
    return generator();
  }

  // Draws a random real number in [0,1)
  double rand_r() {
    return zero_one_generator();
  }

  // Draws a random number from a Gamma(a) distribution
  double randGamma(double a) {
    boost::gamma_distribution<> gamma_dist(a);
    boost::variate_generator<boost::mt19937&, boost::gamma_distribution<> >
    gamma_generator(generator, gamma_dist);
    return gamma_generator();
  }

  // Draws a random number from an Exponential(a) distribution
  double randExponential(double a) {
    boost::exponential_distribution<> exponential_dist(a);
    boost::variate_generator<boost::mt19937&,
          boost::exponential_distribution<> >
          exponential_generator(generator, exponential_dist);
    return exponential_generator();
  }

  // Draws a random vector from a symmetric Dirichlet(a) distribution.
  // The dimension of the vector is determined from output.size().
  void randSymDirichlet(double a, std::vector<double>& output) {
    boost::gamma_distribution<> gamma_dist(a);
    boost::variate_generator<boost::mt19937&, boost::gamma_distribution<> >
    gamma_generator(generator, gamma_dist);
    double total    = 0;
    for (unsigned int i = 0; i < output.size(); ++i) {
      output[i]   = gamma_generator();
      total       += output[i];
    }
    for (unsigned int i = 0; i < output.size(); ++i) {
      output[i]   /= total;
    }
  }

  // Samples from an unnormalized discrete distribution, in the range
  // [begin,end)
  size_t randDiscrete(const std::vector<double>& distrib, size_t begin,
                      size_t end) {
    double totprob  = 0;
    for (size_t i = begin; i < end; ++i) {
      totprob += distrib[i];
    }
    double r        = totprob * zero_one_generator();
    double cur_max  = distrib[begin];
    size_t idx      = begin;
    while (r > cur_max) {
      cur_max += distrib[++idx];
    }
    return idx;
  }

  // Converts the range [begin,end) of a vector of log-probabilities into
  // relative probabilities
  static void logprobsToRelprobs(std::vector<double> &distrib, size_t begin,
                                 size_t end) {
    // Find the maximum element in [begin,end)
    double max_log = *std::max_element(distrib.begin()+begin,
                                       distrib.begin()+end);
    for (size_t i = begin; i < end; ++i) {
      // Avoid over/underflow by centering log-probabilities to their max
      distrib[i] = exp(distrib[i] - max_log);
    }
  }

  // Log-gamma function
  static double lnGamma(double xx) {
    int j;
    double x, y, tmp1, ser;
    static const double cof[6] = { 76.18009172947146, -86.50532032941677,
                                   24.01409824083091, -1.231739572450155,
                                   0.1208650973866179e-2, -0.5395239384953e-5
                                 };
    y = xx;
    x = xx;
    tmp1 = x+5.5;
    tmp1 -= (x+0.5)*log(tmp1);
    ser = 1.000000000190015;
    for (j = 0; j < 6; j++) ser += cof[j]/++y;
    return -tmp1+log(2.5066282746310005*ser/x);
  }
};  // Random

#endif  // defined SHARED_RANDOM_HPP__
