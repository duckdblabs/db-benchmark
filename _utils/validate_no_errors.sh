if [ $(grep -i "error" out/run_*.err | wc -l) = 0 ]
then
	# no true errors found, print last line of each output script
    echo "No Errors found in run_*.err logs"
else
	echo "The following errors have been found. Failing check"
	grep -i "error" out/*.err
	exit 1
fi
# check report generation
if [ $(grep -i "quitting" out/*.out | wc -l) = 0 && $(grep -i "execution halted" out/*.out | wc -l) = 0 ]
then
	exit 0
else
	echo "Report generation failed. Printing output below"
	cat out/rmarkdown_*.out
	exit 1
fi
# errors found


