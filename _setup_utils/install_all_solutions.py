import os
import csv
import sys
import subprocess

SOLUTIONS_FILENAME = "_control/solutions.csv"


INCLUDE = set()

def install_solution(solution_name):
    min_setup_file_name = f"./{solution_name}/min-setup-{solution_name}.sh"
    setup_file_name = f"./{solution_name}/setup-{solution_name}.sh"
    upgrade_file_name = f"./{solution_name}/upg-{solution_name}.sh"
    get_version_filename = f"./{solution_name}/ver-{solution_name}.sh"
    print(f"Installing {solution_name}")
    do_install = False
    try:
        result = subprocess.call([get_version_filename], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        if result != 0:
            do_install = True
    except Exception as e:
        do_install = True

    if do_install:
        if os.path.exists(min_setup_file_name):
            subprocess.call([min_setup_file_name])
        elif os.path.exists(setup_file_name):
            subprocess.call([setup_file_name])
        else:
            # print(f"no script for {setup_file_name} or {min_setup_file_name}")
            raise Exception(f"No script to install {solution_name}")
    else:
        subprocess.call([upgrade_file_name])

# based on the name of the solution, run the {{solution}}/min-setup-{{solution}}.sh file.
# if there is no min-setup-{{solution}}.sh, then run setup-{{solution}}.sh.
# if error, exit with an error
# else don't
def include_all_solutions():
    global INCLUDE
    with open(SOLUTIONS_FILENAME, newline="") as solutions_file:
        solutions = csv.DictReader(solutions_file, delimiter=',')
        for row in solutions:
            if row['solution'] == "data.table":
                INCLUDE.add("datatable")
            else:
                INCLUDE.add(row['solution'])
        
if len(sys.argv) == 0:
    print("""
Usage: python3 install_all_solutions.py solution_name solution_name ...
       python3 install_all_solutions.py all --exclude clickhouse polars
""")
    exit(1)

# first argument is file name

def main():
    global INCLUDE
    including = True
    for solution in sys.argv[1:]:
        if solution.strip() == "all":
            include_all_solutions()
        elif solution.strip() == "--exclude":
            including = False
            continue
        else:
            if including:
                if solution == "data.table":
                    INCLUDE.add("datatable")
                elif solution == "clickhouse":
                    INCLUDE.add("clickhouse")
                    INCLUDE.add("polars")
                else:
                    INCLUDE.add(solution)
            else:
                sol = solution.strip()
                INCLUDE.remove(sol)

    for solution in INCLUDE:
        install_solution(solution)


if __name__ == "__main__":
    main()
    
