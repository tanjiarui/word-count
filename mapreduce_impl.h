#pragma once

#include "master.h"
#include "file_shard.h"
#include "mapreduce_spec.h"

class MapReduceImpl
{
private:
	MapReduceSpec mr_spec_;
	vector<FileShard> file_shards_;
	/* DON'T change the function declaration for these three functions */
	bool read_and_validate_spec(const string& config_filename);
	bool create_shards();
	bool run_master();
public:
	/* DON'T change this function declaration */
	bool run(const string& config_filename);
};
