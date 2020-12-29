# MapReduce Infrastructure

## Install Dependency

[How to setup dependencies](https://github.com/grpc/grpc/blob/master/BUILDING.md)

## How to Run MapReduce Job

**This project can run successfully both on Linux and Mac OS X**

1. First, make sure you already installed gRPC and its dependent `Protocol Buffers v3.0`. Actually, protobuf was included in gRPC, so just install gRPC.

2. Compile code and generate libraries
	- Goto the project directory and run `make` command, two libraries would be created in the lib directory: `libmapreduce.so` and `libmr_worker.so`. Two exe files would also be created: `mrdemo` and `mr_worker`.
 
		```bash
			make
		```

3. **Now running the demo, once you have created all the binaries and libraries.**
	- Clear the files if any in the output directory

		```bash
			rm ./output/*
		```
	- Prepare input files as the example files did in the input directory

	- Start all the worker processes in the following fashion:

		```bash
			./mr_worker localhost:50051 & ./mr_worker localhost:50052 & ./mr_worker localhost:50053 & ./mr_worker localhost:50054
		```
	**Before running, make sure the numbers of worker are identical with worker_ipaddr_ports in the config file**

	- Then start your main map reduce process: `./mrdemo`

		```bash
			./mrdemo
		```

	- Once the ./mrdemo finishes, kill all the worker proccesses you started.
		1. For Mac OS X:

			```bash
				killall mr_worker
			```

		2. For Linux:

			```bash
				killall mr_worker
			```

	- Check output directory to see if you have the correct results(obviously once you have done the proper implementation of your library. There would be some temp files and output files.

# Reference

Jeffrey Dean and Sanjay Ghemawat, [MapReduce: Simplified Data Processing on Large Clusters](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf), 2004
