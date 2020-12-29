#include <mapreduce.h>
#include <unistd.h>
#include <cstdlib>
#include <iostream>

#ifndef PATH_MAX
#define PATH_MAX 200
#endif

int main(int argc, char** argv)
{
	char* cwd;
	char buff[PATH_MAX + 1];
	cwd = getcwd(buff, PATH_MAX + 1);
	if (cwd == NULL)
	{
		cerr << "Failed to retrieve the current directory." << endl;
		return EXIT_FAILURE;
	}
	const string filename = string(cwd) + "/config.ini";

	MapReduce job;
	return job.run(filename) ? EXIT_SUCCESS : EXIT_FAILURE;
}
