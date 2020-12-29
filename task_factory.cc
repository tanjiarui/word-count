#include <task_factory.h>
#include <functional>
#include <unordered_map>
#include <utility>
#include "tasks.h"

BaseMapper::BaseMapper() : impl_(new BaseMapperInternal) {}

BaseMapper::~BaseMapper() {}

void BaseMapper::emit(const string& key, const string& val)
{
	impl_->emit(key, val);
}

BaseReducer::BaseReducer() : impl_(new BaseReducerInternal) {}

BaseReducer::~BaseReducer() {}

void BaseReducer::emit(const string& key, const string& val)
{
	impl_->emit(key, val);
}

/*
construct workers in the factory
client->get_mapper_from_task_factory->TaskFactory& instance->new TaskFactory()->get_mapper
*/
namespace
{
	class TaskFactory
	{
	public:
		static TaskFactory& instance()
		{
			static TaskFactory* instance = new TaskFactory();
			return *instance;
		}
		shared_ptr<BaseMapper> get_mapper(const string& user_id)
		{
			auto itr = mappers_.find(user_id);
			if (itr == mappers_.end()) return nullptr;
			return itr->second();
		}
		shared_ptr<BaseReducer> get_reducer(const string& user_id)
		{
			auto itr = reducers_.find(user_id);
			if (itr == reducers_.end()) return nullptr;
			return itr->second();
		}
		unordered_map<string, function<shared_ptr<BaseMapper>()>> mappers_;
		unordered_map<string, function<shared_ptr<BaseReducer>()>> reducers_;
	};
}

bool register_tasks(string user_id, function<shared_ptr<BaseMapper>()>& generate_mapper, function<std::shared_ptr<BaseReducer>()>& generate_reducer)
{
	TaskFactory& factory = TaskFactory::instance();
	return factory.mappers_.insert(make_pair(user_id, generate_mapper)).second && factory.reducers_.insert(make_pair(user_id, generate_reducer)).second;
}

shared_ptr<BaseMapper> get_mapper_from_task_factory(const string& user_id)
{
	return TaskFactory::instance().get_mapper(user_id);
}

shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id)
{
	return TaskFactory::instance().get_reducer(user_id);
}
