echo "thread_num $1"
workload = "workload/workloada.spec"
for db_name in ${@:2}; do 
	./ycsb -db $db_name -threads $1 -P $workload 2>>$db_name_$filename.output
done