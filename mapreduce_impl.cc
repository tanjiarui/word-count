#include "mapreduce_impl.h"

bool MapReduceImpl::read_and_validate_spec(const string& config_filename)
{
	return read_mr_spec_from_config_file(config_filename, mr_spec_) && validate_mr_spec(mr_spec_);
}
bool MapReduceImpl::create_shards()
{
	return shard_files(mr_spec_, file_shards_);
}
bool MapReduceImpl::run_master()
{
	Master master(mr_spec_, file_shards_);
	return master.run();
}
bool MapReduceImpl::run(const string& config_filename)
{
	if(!read_and_validate_spec(config_filename))
	{
		cerr << "Spec not configured properly." << endl;
		return false;
	}
	if(!create_shards())
	{
		cerr << "Failed to create shards." << endl;
		return false;
	}
	if(!run_master())
	{
		cerr << "MapReduce failure. Something didn't go well!" << endl;
		return false;
	}
	return true;
}
