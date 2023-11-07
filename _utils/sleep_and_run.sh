while [ -f run.lock ]
do
   sleep 1800
done


rm run.lock

./run.sh
