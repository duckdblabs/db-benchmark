#!/usr/bin/env python3

import os
import gc
import sys
import timeit
import logging
import pandas as pd
from dataclasses import dataclass
from typing import List, Any

import dask as dk
import dask.dataframe as dd

# Import needed utilities
THIS_DIR = os.path.abspath(
    os.path.dirname(__file__)
)
HELPERS_DIR = os.path.abspath(
    os.path.join(
        THIS_DIR, '../_helpers'
    )
)
sys.path.extend((THIS_DIR, HELPERS_DIR))
from helpers import *
from common import Query, QueryRunner, dask_client

ver = dk.__version__
git = dk.__git_revision__
task = "join"
solution = "dask"
fun = ".merge"
cache = "TRUE"

logging.basicConfig(
    level=logging.INFO,
    format='{ %(name)s:%(lineno)d @ %(asctime)s } - %(message)s'
)
logger = logging.getLogger(__name__)

class QueryOne(Query):
    question = "small inner on int"

    @staticmethod
    def query(
        x: dd.DataFrame,
        small: dd.DataFrame,
        medium: dd.DataFrame,
        big: dd.DataFrame
    ) -> dd.DataFrame:
        return x.merge(small, on='id1').compute()

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v1'].sum(), ans['v2'].sum()]


class QueryTwo(Query):
    question = "medium inner on int"

    @staticmethod
    def query(
        x: dd.DataFrame,
        small: dd.DataFrame,
        medium: dd.DataFrame,
        big: dd.DataFrame
    ) -> dd.DataFrame:
        return x.merge(medium, on='id2').compute()

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v1'].sum(), ans['v2'].sum()]

class QueryThree(Query):
    question = "medium outer on int"

    @staticmethod
    def query(
        x: dd.DataFrame,
        small: dd.DataFrame,
        medium: dd.DataFrame,
        big: dd.DataFrame
    ) -> dd.DataFrame:
        return x.merge(medium, how='left', on='id2').compute()

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v1'].sum(), ans['v2'].sum()]

class QueryFour(Query):
    question = "medium inner on factor"

    @staticmethod
    def query(
        x: dd.DataFrame,
        small: dd.DataFrame,
        medium: dd.DataFrame,
        big: dd.DataFrame
    ) -> dd.DataFrame:
        return x.merge(medium, on='id5').compute()

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v1'].sum(), ans['v2'].sum()]

class QueryFive(Query):
    question = "big inner on int"

    @staticmethod
    def query(
        x: dd.DataFrame,
        small: dd.DataFrame,
        medium: dd.DataFrame,
        big: dd.DataFrame
    ) -> dd.DataFrame:
        return x.merge(big, on='id3').compute()

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v1'].sum(), ans['v2'].sum()]

def load_datasets(
    data_name: str,
    on_disk: bool,
    data_dir: str = 'data',
    **kwargs
) -> List[dd.DataFrame]:
    """
    Load the datasets into a dask session.

    Args:
        data_name (str): Name of the data to load.
        on_disk (bool): If the data should be loaded from parquet on disk.
        data_dir (str, optional): The directory which to look for the data. Defaults to 'data'.
        **kwargs: Any extra parameters to pass to dask's `read_csv` function.

    Returns:
        List[dd.DataFrame]: primary, small, medium, and big datasets respectively.
    """
    fext = "parquet" if on_disk else "csv"

    src_jn_x = os.path.join(data_dir, data_name+"."+fext)

    y_data_name = join_to_tbls(data_name)
    src_jn_y = [
        os.path.join(data_dir, y_data_name[0]+"."+fext),
        os.path.join(data_dir, y_data_name[1]+"."+fext),
        os.path.join(data_dir, y_data_name[2]+"."+fext)
    ]
    if len(src_jn_y) != 3:
        raise Exception("Something went wrong in preparing files used for join")

    logging.info("Loading dataset: %s" % data_name)

    logging.info("Reading source: %s" % src_jn_x)
    x = dd.read_csv(src_jn_x, engine="pyarrow", **kwargs).persist()

    logging.info("Reading source: %s" % src_jn_y[0])
    small = dd.read_csv(src_jn_y[0], engine="pyarrow", **kwargs).persist()
    logging.info("Reading source: %s" % src_jn_y[1])
    medium = dd.read_csv(src_jn_y[1], engine="pyarrow", **kwargs).persist()
    logging.info("Reading source: %s" % src_jn_y[2])
    big = dd.read_csv(src_jn_y[2], engine="pyarrow", **kwargs).persist()

    return [
        x,
        small,
        medium,
        big
    ]

def run_task(
    data_name: str,
    machine_type: str,
    on_disk: bool,
):
    runner = QueryRunner(
        task=task,
        solution=solution,
        solution_version=ver,
        solution_revision=git,
        fun=fun,
        cache=cache,
        on_disk=on_disk
    )

    x, small, medium, big = load_datasets(data_name, on_disk)
    in_rows = len(x)
    logger.info(f"X dataset rows: {in_rows:,}")
    logger.info(f"Small dataset rows: {len(small.index):,}")
    logger.info(f"Medium dataset rows: {len(medium.index):,}")
    logger.info(f"Big dataset rows: {len(big.index):,}")

    task_init = timeit.default_timer()
    logger.info("Joining...")

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        args=[x, small, medium, big],
        query=QueryOne,
        machine_type=machine_type
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        args=[x, small, medium, big],
        query=QueryTwo,
        machine_type=machine_type
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        args=[x, small, medium, big],
        query=QueryThree,
        machine_type=machine_type
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        args=[x, small, medium, big],
        query=QueryFour,
        machine_type=machine_type
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        args=[x, small, medium, big],
        query=QueryFive,
        machine_type=machine_type
    )

    logger.info("Grouping finished, took %0.fs" % (timeit.default_timer()-task_init))

if __name__ == '__main__':
    logging.info("# join-dask.py")
    data_name = os.environ['SRC_DATANAME']
    machine_type = os.environ.get('MACHINE_TYPE', 'local')
    on_disk = data_name.split("_")[1] == "1e10" or (data_name.split("_")[1] == "1e9" and os.environ["MACHINE_TYPE"] == "c6id.4xlarge")

    na_flag = int(data_name.split("_")[3])
    sort_flag = int(data_name.split("_")[4])
    if na_flag > 0 or sort_flag > 0:
        logger.error("skip due to na_flag>0 or sort_flag>0: dask/dask#7015", flush=True, file=sys.stderr)
        exit(0) # dask/dask#7015

    run_task(
        data_name=data_name,
        machine_type=machine_type,
        on_disk=on_disk,
    )