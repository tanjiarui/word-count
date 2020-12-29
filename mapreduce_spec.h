#pragma once

#include <assert.h>
#include <string.h>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>

using namespace std;

struct MapReduceSpec
{
	size_t workerNums;			// # of workers
	size_t outputNums;			// # of output files
	size_t mapSize;				// map kilobytes
	string outputDir;			// output directory
	string userId;				// user id
	vector<string> workerAddrs;	// worker addresses
	vector<string> inputFiles;	// input files
};

/* populate MapReduceSpec data structure with the specification
 * from the config file */
inline bool read_mr_spec_from_config_file(const string& config_filename, MapReduceSpec& mr_spec)
{
	unordered_map<string, vector<string>> map;
	ifstream myfile(config_filename);
	if(myfile.is_open())
	{
		string line;
		char headStr[200];
		char tailStr[200];
		char comp[200];
		while (getline(myfile, line))
		{
			sscanf(line.c_str(), "%[A-Z,a-z,_]=%s", headStr, tailStr);
			istringstream input(tailStr);
			while (input.getline(comp, 200, ','))
				map[headStr].push_back(comp);
		}
		myfile.close();
	}
	else
	{
		cerr << "Failed to open file " << config_filename << endl;
		return false;
	}

	// read specifications from config file and store them into structure MapReduceSpec
	mr_spec.outputDir = map["output_dir"][0];
	mr_spec.userId = map["user_id"][0];
	mr_spec.workerNums = atoi(map["n_workers"][0].c_str());
	mr_spec.outputNums = atoi(map["n_output_files"][0].c_str());
	mr_spec.mapSize = atoi(map["map_kilobytes"][0].c_str());
	mr_spec.workerAddrs = move(map["worker_ipaddr_ports"]);
	mr_spec.inputFiles = move(map["input_files"]);

	return true;
}

/* validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec)
{
	assert(mr_spec.workerNums > 0 && mr_spec.workerNums == mr_spec.workerAddrs.size());
	assert(mr_spec.outputNums > 0 && mr_spec.mapSize > 0);
	assert(mr_spec.outputDir.c_str() != nullptr);
	assert(mr_spec.userId.c_str() != nullptr);

	// validate input file path
	for(auto& input : mr_spec.inputFiles)
	{
		ifstream myfile(input);
		assert(myfile.is_open());
		myfile.close();
	}

	// validate worker address port
	char hostName[50];
	char port[50];
	for(auto& addr : mr_spec.workerAddrs)
	{
		sscanf(addr.c_str(), "%[a-z,]:%[0-9]", hostName, port);
		assert(atoi(port) <= 65535);
		assert(strncmp(hostName, "localhost", 9) == 0);
	}

	return true;
}
