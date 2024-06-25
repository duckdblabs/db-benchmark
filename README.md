# Forked from [h2oai/db-benchmark](https://github.com/h2oai/db-benchmark)

Repository for reproducible benchmarking of database-like operations in single-node environment.  
Benchmark report is available at [duckdblabs.github.io/db-benchmark](https://duckdblabs.github.io/db-benchmark).  
We focused mainly on portability and reproducibility. Benchmark is routinely re-run to present up-to-date timings. Most of solutions used are automatically upgraded to their stable or development versions.  
This benchmark is meant to compare scalability both in data volume and data complexity.  
Contribution and feedback are very welcome!  

# Tasks

  - [x] groupby
  - [x] join
  - [x] groupby2014

# Solutions

  - [x] [dask](https://github.com/dask/dask)
  - [x] [data.table](https://github.com/Rdatatable/data.table)
  - [x] [collapse](https://sebkrantz.github.io/collapse/)
  - [x] [dplyr](https://github.com/tidyverse/dplyr)
  - [x] [pandas](https://github.com/pandas-dev/pandas)
  - [x] [(py)datatable](https://github.com/h2oai/datatable)
  - [x] [spark](https://github.com/apache/spark)
  - [x] [ClickHouse](https://github.com/yandex/ClickHouse)
  - [x] [Polars](https://github.com/ritchie46/polars)
  - [x] [Arrow](https://github.com/apache/arrow)
  - [x] [DuckDB](https://github.com/duckdb/duckdb)
  - [x] [DuckDB-latest](https://github.com/duckdb/duckdb)
  - [x] [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl)
  - [x] [In Memory DataSets](https://github.com/sl-solution/InMemoryDatasets.jl)
  - [x] [Datafusion](https://github.com/apache/arrow-datafusion)

If you would like your solution to be included, feel free to file a PR with the necessary setup-_solution_/ver-_solution_/groupby-_solution_/join-_solution_ scripts. If the team at DuckDB Labs approves the PR it will be merged. In the interest of transparency and fairness, only results from open-source data-science tools will be merged.

# Reproduce

## Batch benchmark run

- if solution uses python create new `virtualenv` as `$solution/py-$solution`, example for `pandas` use `virtualenv pandas/py-pandas --python=/usr/bin/python3.10`
- install every solution, follow `$solution/setup-$solution.sh` scripts by hand, they are not automatic scripts.
- edit `run.conf` to define solutions and tasks to benchmark
- generate data, for `groupby` use `Rscript _data/groupby-datagen.R 1e7 1e2 0 0` to create `G1_1e7_1e2_0_0.csv`, re-save to binary format where needed (see below), create `data` directory and keep all data files there
- edit `_control/data.csv` to define data sizes to benchmark using `active` flag
- ensure SWAP is disabled and ClickHouse server is not yet running
- start benchmark with `./run.sh`

## Single solution benchmark

- install solution software
  - for python we recommend to use `virtualenv` for better isolation
  - for R ensure that library is installed in a solution subdirectory, so that `library("dplyr", lib.loc="./dplyr/r-dplyr")` or `library("data.table", lib.loc="./datatable/r-datatable")` works
  - note that some solutions may require another to be installed to speed-up csv data load, for example, `dplyr` requires `data.table` and similarly `pandas` requires (py)`datatable`
- generate data using `_data/*-datagen.R` scripts, for example, `Rscript _data/groupby-datagen.R 1e7 1e2 0 0` creates `G1_1e7_1e2_0_0.csv`, put data files in `data` directory
- run benchmark for a single solution using `./_launcher/solution.R --solution=data.table --task=groupby --nrow=1e7`
- run other data cases by passing extra parameters `--k=1e2 --na=0 --sort=0`
- use `--quiet=true` to suppress script's output and print timings only, using `--print=question,run,time_sec` specify columns to be printed to console, to print all use `--print=*`
- use `--out=time.csv` to write timings to a file rather than console

## Running script interactively

- install software in expected location, details above
- ensure data name to be used in env var below is present in `./data` dir
- source python virtual environment if needed
- call `SRC_DATANAME=G1_1e7_1e2_0_0 R`, if desired replace `R` with `python` or `julia`
- proceed pasting code from benchmark script

# Updating the benchmark.

The benchmark will now be updated upon request. A request can be made by creating a PR with a combination of the following.

The PR **must** include 
- updates to the time.csv and log.csv files of a run on a c6id.metal machine. If you are re-enabling a query for a solution, you can just include new times and logs for the query, however, the version must match currently reported version.

The PR must include **one** of the following
- changes to a solution VERSION file.
- changes to a solution groupby or join script. This can mean:
  1. Loading the data differently 
  2. Changing settings for a solution.
  3. Re-enabling a query for a solution

To facilitate creating an instance identical to the one with the current results, the script `_utils/format_and_mount.sh`  was created. The script does the following 
1. Formats and mounts an nvme drive so that solutions have access to instance storage
2. Creates a new directory `db-benchmark-metal` on the nvme drive. This directory is a clone of the repository. Having a clone of the benchmark on the nvme drive enables the solutions to load the data faster (assuming you follow the steps to copy the data onto the nvme mount). 

Once the `db-benchmark-metal` directory is created, you will need to 
1. Create or generate all the datasets. The benchmark will not be updated if only a subset of datasets are tested. 
  - If you call `./_utils/format_and_mount.sh -c` the datasets will be created for you. Creating every dataset will take at least >1hr
2. Install the solutions you wish to have updated. The {{solution}}/setup-{{solution}}.sh should have everything you need
3. Update the solution(s) groupby or join scripts with any desired changes
4. Benchmark on your solution against all datasets.
5. Generate the report to see how the results compare to other solutions. The report should be automatically generated. You can find it in `public`.
6. Create your PR! Include the updates to the time.csv and logs.csv files

The PR will then be reviewed by the DuckDB Labs team where we will run the benchmark again ourselves to validate the new results. If there aren't any questions, we will merge the PR and publish a new report!


# Example environment

- setting up c6id.metal: 250GB RAM, 128 cores: [Amazon link](https://aws.amazon.com/ec2/instance-types/)  
- Full reproduce script on clean Ubuntu 22.04: [_utils/repro.sh](https://github.com/duckdblabs/db-benchmark/blob/master/_utils/repro.sh)

# Acknowledgment

Timings for solutions from before the fork have been deleted. You can still view them on the original [h2oai/db-benchmark fork](https://github.com/h2oai/db-benchmark). Including these timings in report generation resulted in errors, and since all libraries have been updated and benchmarked using new hardware, the decision was made to start a new results file. Timings for some solutions might be missing for particular data sizes or questions. Some functions are not yet implemented in all solutions so we were unable to answer all questions in all solutions. Some solutions might also run out of memory when running benchmark script which results the process to be killed by OS. There is also a timeout for single benchmark script to run, once the timeout value is reached script is terminated.
Please check [_exceptions_](https://github.com/h2oai/db-benchmark/issues?q=is%3Aissue+is%3Aopen+label%3Aexceptions) label in the original h2oai repository for a list of issues/defects in solutions, that makes us unable to provide all timings.
There is also [_no documentation_](https://github.com/h2oai/db-benchmark/labels/no%20documentation) label that lists issues that are blocked by missing documentation in solutions we are benchmarking.

# Notice

In the interest of transparency and fairness, only results from open-source data-science tools will be included in the benchmark. 
