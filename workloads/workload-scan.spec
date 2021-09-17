# Yahoo! Cloud System Benchmark
# Workload-read: Read only workload
#   Application example: Session store recording recent actions
#
#   Scan ratio: 100
#   Default data size: 256 B records (8 fields, 32 bytes each, plus key)
#   Request distribution: uniform

recordcount=160000000
operationcount=1000000
fieldcount=8
fieldlength=32
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=0
scanproportion=1
insertproportion=0

requestdistribution=uniform