#include <task_factory.h>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <numeric>

class UserMapper : public BaseMapper
{
public:
	void map(const string& input_line) override
	{
		char* c_input = new char[input_line.length() + 1];
		strcpy(c_input, input_line.c_str());
		static const char* delims = " ,.\"'";
		char* start = strtok(c_input, delims);
		while (start != NULL)
		{
			emit(start, "1");
			start = strtok(NULL, delims);
		}
	}
};

class UserReducer : public BaseReducer
{
public:
	void reduce(const string& key, const vector<string>& values) override
	{
		vector<int> counts;
		transform(values.cbegin(), values.cend(), back_inserter(counts), [](const string numstr) { return atoi(numstr.c_str()); });
		emit(key, to_string(accumulate(counts.begin(), counts.end(), 0)));
	}
};

static function<shared_ptr<BaseMapper>()> my_mapper = []()
{
	return shared_ptr<BaseMapper>(new UserMapper);
};
static function<shared_ptr<BaseReducer>()> my_reducer = []()
{
	return shared_ptr<BaseReducer>(new UserReducer);
};
namespace
{
	bool register_tasks_and_check()
	{
		const string user_id = "mapreduce";
		if (!register_tasks(user_id, my_mapper, my_reducer))
		{
			cout << "Failed to register user_id: " << user_id << endl;
			exit(EXIT_FAILURE);
		}
		return true;
	}
}
bool just_store = register_tasks_and_check();
