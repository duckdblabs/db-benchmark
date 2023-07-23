if [ $(grep -i "error" out/*.err | wc -l) = 0 ]
then
	# no true errors found, print last line of each output script
        tail -n 1 out/*.out
	exit 0;
fi
# errors found
echo "The following errors have been found. Failing check"
grep -i "error" out/*.err
exit 1
