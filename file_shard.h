#pragma once

#include <climits>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <cmath>
#include <vector>
#include "mapreduce_spec.h"

#define FILE_NAME_MAX_LEN 100

/* create your own data structure here, where you can hold
   information about file splits that your master would use for its own book keeping and to convey the tasks
   to the workers for mapping */
struct FileShard
{
	unordered_map<string, pair<streampos, streampos>> shardsMap;
};

// return total input files size in bytes
inline uint64_t get_totoal_size(const MapReduceSpec& mr_spec)
{
	uint64_t total_size = 0;
	for (auto& input : mr_spec.inputFiles)
	{
		ifstream myfile(input, ios::binary);
		myfile.seekg(0, ios::beg);
		streampos begin = myfile.tellg();
		myfile.seekg(0, ios::end);
		streampos end = myfile.tellg();
		total_size += (end - begin + 1);
		myfile.close();
	}
	return total_size;
}

// return input file size in bytes
inline size_t get_input_size(ifstream& myfile)
{
	myfile.seekg(0, ios::beg);
	streampos begin = myfile.tellg();

	myfile.seekg(0, ios::end);
	streampos end = myfile.tellg();
	return (end - begin + 1);
}

/* create file shards from the list of input files, map_kilobytes
   etc. using mr_spec you populated */
inline bool shard_files(const MapReduceSpec& mr_spec, vector<FileShard>& fileShards)
{
	uint64_t total_size = get_totoal_size(mr_spec);
	size_t shardNums = std::ceil(total_size / (mr_spec.mapSize * 1024.0)) + 1;
	fileShards.reserve(shardNums);

	for (auto& input : mr_spec.inputFiles)
	{
		ifstream myfile(input, ios::binary);
		uint64_t file_size = get_input_size(myfile);

		cout << "\nSplit file : " << input << " " << file_size << " Bytes into shards ...\n";
		streampos offset = 0;
		uint64_t rest_size = file_size;
		while(rest_size > 0)
		{
			// find offset begin for a shard
			myfile.seekg(offset, ios::beg);
			streampos begin = myfile.tellg();

			// find offset end for a shard
			myfile.seekg(mr_spec.mapSize * 1024, ios::cur);
			streampos end = myfile.tellg();

			// if offset exceed size, set its end position
			if (end >= file_size)
				myfile.seekg(0, ios::end);
			else
				// find closest '\n' delimit
				myfile.ignore(LONG_MAX, '\n');
			end = myfile.tellg();

			size_t chunk_size = (end - begin + 1);
			cout << "Process offset (" << begin << "," << end << ") " << chunk_size << " bytes into shard ...\n";

			// store chunk into shards
			FileShard temp;
			temp.shardsMap[input] = make_pair(begin, end);
			fileShards.push_back(std::move(temp));

			rest_size -= chunk_size;
			offset = static_cast<int>(end) + 1;
		}
		myfile.close();
	}

	return true;
}
