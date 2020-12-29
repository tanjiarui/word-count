#pragma once

#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

// save intermediate files
struct BaseMapperInternal
{
	/* NOW you can add below, data members and member functions as per the need of
	* your implementation*/
	mutex mutex_;
	string output_dir_;
	unordered_set<string> temp_files_;
	int output_num_;

	inline string hash2key(const string& key)
	{
		hash<string> hash_func;
		string temp_file = "output/temp" + to_string(hash_func(const_cast<string&>(key)) % output_num_) + ".txt";
		return temp_file;
	}
	/* DON'T change this function's signature */
	inline void emit(const string& key, const string& val)
	{
		// periodically, write results of lines into intermediate files.
		lock_guard<mutex> lock(mutex_);
		string filename = hash2key(key);
		ofstream myfile(filename, ios::app);
		if (myfile.is_open())
		{
			myfile << key << " " << val << endl;
			myfile.close();
		}
		else
		{
			cerr << "Failed to open file " << filename << endl;
			exit(-1);
		}
		temp_files_.insert(filename);
	}
};

// save final files
struct BaseReducerInternal
{
	/* NOW you can add below, data members and member functions as per the need of
	* your implementation*/
	int file_number_;
	mutex mutex_;
	string output_dir_;

	inline void emit(const string& key, const string& val)
	{
		lock_guard<std::mutex> lock(mutex_);
		string filename = "output/output" + to_string(file_number_) + ".txt";
		ofstream myfile(filename, ios::app);
		if (myfile.is_open())
		{
			myfile << key << " " << val << endl;
			myfile.close();
		}
		else
		{
			cerr << "Failed to open file " << filename << endl;
			exit(-1);
		}
	}
};
