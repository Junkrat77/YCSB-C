# Yahoo! Cloud System Benchmark
# Workload-read: Read only workload
#   Application example: Session store recording recent actions
#
#   Read/update ratio: 100/0
#   Default data size: 256 B records (8 fields, 32 bytes each, plus key)
#   Request distribution: uniform

recordcount=160000000
operationcount=32000000
#recordcount=1600000
#operationcount=320000
fieldcount=8
fieldlength=32
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=uniform

