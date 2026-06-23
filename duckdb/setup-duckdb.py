#!/usr/bin/env python3
# Sets up the python virtual environment used to run the duckdb benchmark and
# installs the latest released version of the duckdb python package into it.
# Replaces the old R based setup (setup-duckdb.sh).

import os
import subprocess
import sys

solution = "duckdb"
venv_dir = os.path.join(solution, "py-%s" % solution)


def run(cmd):
  print("+ " + " ".join(cmd), flush=True)
  subprocess.run(cmd, check=True)


def main():
  # create a fresh virtual environment at duckdb/py-duckdb
  run([sys.executable, "-m", "venv", "--clear", venv_dir])

  venv_python = os.path.join(venv_dir, "bin", "python3")

  # install / upgrade the latest released duckdb plus runtime deps
  run([venv_python, "-m", "pip", "install", "--upgrade", "pip"])
  run([venv_python, "-m", "pip", "install", "--upgrade", "psutil"])
  run([venv_python, "-m", "pip", "install", "--upgrade", "duckdb"])

  # write VERSION and REVISION metadata files used by the launcher
  write_version(venv_python)


def write_version(venv_python):
  code = (
    "import duckdb;"
    "open('duckdb/VERSION','w').write(duckdb.__version__);"
    "open('duckdb/REVISION','w').write("
    "duckdb.connect().execute('SELECT source_id FROM pragma_version()').fetchone()[0]);"
  )
  run([venv_python, "-c", code])
  print("duckdb VERSION/REVISION files written", flush=True)


if __name__ == "__main__":
  main()
