import sys
import gc
import os
import logging
import timeit
from abc import ABC, abstractmethod
from typing import Iterable, Any

import dask.dataframe as dd
from dask import distributed

logging.basicConfig(
    level=logging.INFO,
    format='{ %(name)s:%(lineno)d @ %(asctime)s } - %(message)s'
)
logger = logging.getLogger(__name__)

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

class Query(ABC):
    question: str = None

    @staticmethod
    @abstractmethod
    def query(*args) -> dd.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def check(ans: dd.DataFrame) -> Any:
        pass

    @classmethod
    def name(cls) -> str:
        return f"{cls.__name__}: {cls.question}"

class QueryRunner:
    def __init__(
        self,
        task: str,
        solution: str,
        solution_version: str,
        solution_revision: str,
        fun: str,
        cache: str,
        on_disk: bool
    ):
        self.task = task
        self.solution = solution
        self.solution_version = solution_version
        self.solution_revision = solution_revision
        self.fun = fun
        self.cache = cache
        self.on_disk = on_disk

    def run_query(
        self,
        data_name: str,
        in_rows: int,
        args: Iterable[Any],
        query: Query,
        machine_type: str,
        runs: int = 2,
        raise_exception: bool = False,
    ):
        logger.info("Running '%s'" % query.name())

        try:
            for run in range(1, runs+1):
                gc.collect() # TODO: Able to do this in worker processes? Want to?

                # Calculate ans
                t_start = timeit.default_timer()
                ans = query.query(*args)
                logger.debug("Answer shape: %s" % (ans.shape, ))
                t = timeit.default_timer() - t_start
                m = memory_usage()

                logger.info("\tRun #%s: %0.3fs" % (run, t))

                # Calculate chk
                t_start = timeit.default_timer()
                chk = query.check(ans)
                chkt = timeit.default_timer() - t_start


                write_log(
                    task=self.task,
                    data=data_name,
                    in_rows=in_rows,
                    question=query.question,
                    out_rows=ans.shape[0],
                    out_cols=ans.shape[1],
                    solution=self.solution,
                    version=self.solution_version,
                    git=self.solution_revision,
                    fun=self.fun,
                    run=run,
                    time_sec=t,
                    mem_gb=m,
                    cache=self.cache,
                    chk=make_chk(chk),
                    chk_time_sec=chkt,
                    on_disk=self.on_disk,
                    machine_type=machine_type
                )
                if run == runs:
                    # Print head / tail on last run
                    logger.debug("Answer head:\n%s" % ans.head(3))
                    logger.debug("Answer tail:\n%s" % ans.tail(3))
                del ans
        except Exception as err:
            logger.error("Query '%s' failed!" % query.name())
            print(err)

            # Re-raise if instructed
            if raise_exception:
                raise err

def dask_client() -> distributed.Client:
    # we use process-pool instead of thread-pool due to GIL cost
    return distributed.Client(processes=True, silence_logs=logging.ERROR)
