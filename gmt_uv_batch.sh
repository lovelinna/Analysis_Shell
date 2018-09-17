#!/bin/bash

date=`date +"%s"`
yestoday=$(($date-86400))
yestoday_date=`date -d @$yestoday +"%Y-%m-%d"`
echo "submit gmt task with param: "$yestoday_date" 0.0"
/opt/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class rtbanaly.batch.GmtBatchAnalysis  --total-executor-cores 4 --executor-cores 1 --executor-memory 3g  /home/ubuntu/spark_app/adstailor-analysis-filter.jar $yestoday_date 0.0
mysql -uadstailor -hadstailor.cz4ghpyk5xzz.ap-south-1.rds.amazonaws.com -pL1QWsUKQqTuGqFEtn5Srs prod_adstailor_statistics_batch -e "UPDATE prod_adstailor_statistics.demand_dailysummary t1, demand_dailysummary t2   set t1.uniq_clks=case when t1.clks > t2.uniq_clks then t2.uniq_clks else t1.clks end, t1.uniq_imps=case when t1.imps > t2.uniq_imps then t2.uniq_imps else t1.imps end, t1.noad_uniq_imps=t2.noad_uniq_imps, t1.noad_uniq_clks=t2.noad_uniq_clks WHERE t1.dated = '$yestoday_date' and t2.dated= '$yestoday_date' and t1.unionk = t2.unionk"
