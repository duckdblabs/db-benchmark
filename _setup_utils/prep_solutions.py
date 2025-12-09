import sys
import os
import csv

SOLUTIONS_FILENAME = "_control/solutions.csv"
RUN_CONF_FILENAME = "run.conf"

SKIPPED_SOLUTIONS = []


def print_usage():
    print("Usage: python3 _utils/prep_solutions.py --task=[groupby|join]")
    exit(1)

def parse_task():
    task = None
    for arg in sys.argv[1:]:
        if arg.startswith("--task="):
            task = arg.replace("--task=", "")
    if task == None or (task != "groupby" and task != "join"):
        print_usage()
    return task

def parse_solution():
    solution = None
    for arg in sys.argv[1:]:
        if arg.startswith("--solution="):
            solution = arg.replace("--solution=", "")
    return solution

def main():
    task = parse_task()
    solution = parse_solution()
    if solution == "all":
        solution = get_solutions(task)
    if solution == "clickhouse":
        solution = "clickhouse polars"
    update_run_conf_solutions(solution, task)

def update_run_conf_solutions(solution_name_list, task):
    # change what solutions are run in run.conf
    os.system(f"sed 's/export RUN_SOLUTIONS=.*/export RUN_SOLUTIONS=\"{solution_name_list}\"/g' run.conf > run_2.conf")
    os.system(f"sed 's/export RUN_TASKS=.*/export RUN_TASKS=\"{task}\"/g' run_2.conf > run_3.conf")
    os.system(f"sed 's/export DO_REPORT=.*/export DO_REPORT=false/g' run_3.conf > run.conf")
    os.remove('run_2.conf')
    os.remove('run_3.conf')

def get_solutions(task):
    solutions_for_task = ""
    with open(SOLUTIONS_FILENAME, newline="") as solutions_file:
        solutions = csv.DictReader(solutions_file, delimiter=',')
        for row in solutions:
            if row['task'] == task and row['solution'] not in SKIPPED_SOLUTIONS:
                solutions_for_task += row['solution'] + " "
    return solutions_for_task.strip()


if __name__ == "__main__":
    main()