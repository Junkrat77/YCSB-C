# Yahoo! Cloud System Benchmark
# Workload-read: Read only workload
#   Application example: Session store recording recent actions
#
#   Delete ratio: 100
#   Default data size: 256 B records (8 fields, 32 bytes each, plus key)
#   Request distribution: uniform

recordcount=160000000
operationcount=1000000

#recordcount=160000
#operationcount=32000

fieldcount=8
fieldlength=32
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

requestdistribution=uniform