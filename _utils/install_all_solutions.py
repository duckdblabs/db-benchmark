import os
import csv
import sys

SOLUTIONS_FILENAME = "_control/solutions.csv"


def install_solution(solution_name):
    min_setup_file_name = f"./{solution_name}/min-setup-{solution_name}.sh"
    setup_file_name = f"./{solution_name}/setup-{solution_name}.sh"
    print(f"Installing {solution_name}")
    if os.path.exists(min_setup_file_name):
        os.system(min_setup_file_name)
    elif os.path.exists(setup_file_name):
        os.system(setup_file_name)
    else:
        # print(f"no script for {setup_file_name} or {min_setup_file_name}")
        raise Exception(f"No script to install {solution_name}")

# based on the name of the solution, run the {{solution}}/min-setup-{{solution}}.sh file.
# if there is no min-setup-{{solution}}.sh, then run setup-{{solution}}.sh.
# if error, exit with an error
# else don't
def install_all_solutions():
    install_solutions = set()
    with open(SOLUTIONS_FILENAME, newline="") as solutions_file:
        solutions = csv.DictReader(solutions_file, delimiter=',')
        for row in solutions:
            if row['solution'] == "data.table":
                install_solutions.add("datatable")
            else:
                install_solutions.add(row['solution'])
    for solution in install_solutions:
        install_solution(solution)
        
if len(sys.argv) == 0:
    print("Usage: python3 install_all_solutions.py solution_name solution_name ...")
    exit(1)

# first argument is file name
for solution in sys.argv[1:]:
    if solution.strip() == "all":
        install_all_solutions()
    else:
        if solution == "data.table":
            install_solution("datatable")
        elif solution == "clickhouse":
            install_solution("clickhouse")
            install_solution("polars")
        else:
            install_solution(solution)
        
