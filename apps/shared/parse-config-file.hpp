#ifndef __SHARED_PARSE_CONFIG_FILE_HPP__
#define __SHARED_PARSE_CONFIG_FILE_HPP__

/*
 * Copyright (C) 2014 by Carnegie Mellon University.
 */

#include <boost/program_options.hpp>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>

#include <vector>
#include <string>
#include <fstream>

using std::vector;
using std::string;
using std::cout;
using std::cerr;
using std::endl;
using std::stringstream;

namespace po = boost::program_options;

po::variables_map vm;

void parse_hostfile(std::string hostfile, std::vector<std::string>& hostlist) {
  std::ifstream is(hostfile.c_str());
  std::string line;
  hostlist.clear();
  while (!!getline(is, line)) {
    hostlist.push_back(line);
  }
  is.close();
}

void print_parsed_options(void);

void parse_command_line_and_config_file(int argc,
                                        char **argv,
                                        po::options_description &desc) {
  // add --config to suck in the config file
  string arg_file;
  desc.add_options()
    ("arg_file",
     po::value<string>(&arg_file)->default_value(""),
     "argument file");

  // parse the command line
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  // we are interested only in the --config option here
  if (arg_file == "") {
    return;
  }

  // parse the .ini file.  Command line takes precedence, being parse first
  std::ifstream config_in(arg_file.c_str());
  po::store(po::parse_config_file(config_in, desc), vm);
  po::notify(vm);
}

void print_parsed_options(void) {
  cout << "Dumping parsed options:" << endl;
  for (po::variables_map::iterator i = vm.begin(); i != vm.end() ; ++i) {
    const po::variable_value& v = i->second;
    cout << "option " << i->first.c_str() << ": ";
    if (!v.empty()) {
      const ::std::type_info& type = v.value().type();

      // Note: The following variables are used to curcumvent
      // a google c++ lint bug.
      // The bug: in "if (type == typeid(T))", if T is a basic c++ type,
      // such as int, double or bool, goole lint will emit error
      // "all parameter should be named" because it thinks typeid(double)
      // as a function delaration missing the parameter's name.
      // Since typeid takes expressions, using a variable of the desired
      // type works around the problem.
      int intVar = 0;
      double doubleVar = 0;
      float floatVar = 0;
      bool boolVar = false;

      if (type == typeid(string)) {
        const string val = v.as<string>();
        cout << val << endl;
      } else if (type == typeid(intVar)) {
        const int val = v.as<int>();
        cout << val << endl;
      } else if (type == typeid(uint32_t)) {
        const uint32_t val = v.as<uint32_t>();
        cout << val << endl;
      } else if (type == typeid(doubleVar)) {
        const double val = v.as<double>();
        cout << val << endl;
      } else if (type == typeid(floatVar)) {
        const float val = v.as<float>();
        cout << val << endl;
      } else if (type == typeid(boolVar)) {
        const bool val = v.as<bool>();
        cout << val << endl;
      } else {
        cerr << "Missing handling code for this option's type." << endl;
        assert(0);
      }
    } else {
      cerr << "Missing value for this option." << endl;
      assert(0);
    }
  }
}

#endif  // defined __SHARED_PARSE_CONFIG_FILE_HPP__
