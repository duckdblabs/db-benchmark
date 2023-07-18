source ./datafusion/py-datafusion/bin/activate
python3 -c 'import datafusion as df; open("datafusion/VERSION","w").write(df.__version__); open("datafusion/REVISION","w").write("");' > /dev/null
