#!/bin/bash
start_date=$1
end_date=$2
start_ts=`date -d $start_date +"%s"`
end_ts=`date -d $end_date +"%s"`
while [[ $start_ts -le $end_ts ]];
do
	date=`date -d @$start_ts +"%Y-%m-%d"`
	echo "submit gmt task with param: "$date" 0.0"
	/opt/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class rtbanaly.batch.GmtBatchAnalysis  --total-executor-cores 4 --executor-cores 1 --executor-memory 3g  /home/ubuntu/spark_app/adstailor-analysis-filter.jar $date 0.0
	start_ts=$(($start_ts+86400))
done
