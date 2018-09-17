#!/bin/bash

get_log_range(){
        #declare -A log_range=()
        timestamp=$1
        for i in {0..26};
        do
                log_range[$i]=`date -d @$timestamp +%Y%m%d%H`
                timestamp=$(($timestamp-3600))
        done
        #for j in ${log_range[@]};
        #do echo $j
        #done
        #echo ${log_range[@]}   
}

submit_spark_task(){
	echo $curtime"--------submit task with param: "$date" "$log_range_str" "$key
	/opt/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class rtbanaly.batch.AdxBatchAnalysis  --total-executor-cores 4 --executor-cores 1 --executor-memory 3g  /home/ubuntu/spark_app/adstailor-analysis-filter.jar $date $log_range_str $key
	mysql -uadstailor -hadstailor.cz4ghpyk5xzz.ap-south-1.rds.amazonaws.com -pL1QWsUKQqTuGqFEtn5Srs prod_adstailor_statistics_batch -e "UPDATE prod_adstailor_statistics.demand_dailysummary_tz t1, demand_dailysummary_tz t2   set t1.uniq_clks=case when t1.clks > t2.uniq_clks then t2.uniq_clks else t1.clks end, t1.uniq_imps=case when t1.imps > t2.uniq_imps then t2.uniq_imps else t1.imps end, t1.noad_uniq_imps=t2.noad_uniq_imps, t1.noad_uniq_clks=t2.noad_uniq_clks WHERE t1.dated = '$date' and t2.dated= '$date' and t1.unionk = t2.unionk"
}

prepaire_for_task(){
	echo $curtime"--------come into prepair for task"
        start_timestamp=`date +"%s"`
	for key in ${!tzmap[@]};
          do
                date=${tzmap["$key"]}
  		diff_stamp=$(echo "scale=1;($key-2)*3600" | bc|awk -F '.' '{print $1}')
          	new_date=`date -d @"$(($start_timestamp+$diff_stamp))" +'%F'`
 # 		echo "date="$date" newdate="$new_date
                  if [[ $date = "" ]]; then
  			tzmap["$key"]="$new_date"
  		  elif [[ $new_date != $date ]]; then
                          activate_timestamp=$(($start_timestamp-86400))
                          expire_timestamp=$(($start_timestamp))
                          get_log_range $start_timestamp
                          log_range_str=""
                          for i in ${log_range[@]};
                          do
                                  if [[ $log_range_str == "" ]]; then
                                          log_range_str=${i}
                                  else
                                          log_range_str=${log_range_str}","${i}
                                  fi;
                          done
		          sql="select count(*) from at_campaign where (activate_time >= "$activate_timestamp" and activate_time < "$expire_timestamp") or (activate_time < "$activate_timestamp" and expire_time>="$activate_timestamp")"
                          echo $sql
			  campaigns=`$MYSQL -e "$sql" 2>/dev/null|awk 'NR>1'`
                          if [[ $campaigns -gt 0 && ${#log_range[@]} -gt 0 ]]; then
				 #暂时设置1h执行一次，不考虑两个任务同时提交的场景 
				 #status=`cat $data_file|grep $timezone|grep |awk '{print $3}'`
					
                                  submit_spark_task
                                  tzmap["$key"]="$new_date"
			  else
				 echo $curtime"--------there is no task to submit with timezone="$key" and date="$date" and campaigns="$campaigns
                          fi;
		  else
			echo $curtime"--------there is no task to submit with timezone="$key" and date="$date"  because save_date="$date" and new_date="$new_date
                  fi;
          done;
	#清空数据文件，重新存储
	cat /dev/null > $data_file
	for i in ${!tzmap[@]};
        do
        echo $i" "${tzmap["$i"]} >> $data_file
        done

}

curtime=`date +'%Y-%m-%d %H:%M:%S'`
MYSQL="mysql -uadstailor -hadstailor.cz4ghpyk5xzz.ap-south-1.rds.amazonaws.com -pL1QWsUKQqTuGqFEtn5Srs adstailor_adserver"
result=`$MYSQL -e "select distinct(timezone) from at_campaign" 2>/dev/null|awk 'NR>1'`
tz=$(echo $result|tr -d '\n','\t','')
declare -A tzmap=()
data_file=/home/ubuntu/spark_app/data/uv_cache

echo $curtime"-------come into check timezone is zero clock"

for timezone in $tz
do
  tzmap[$timezone]=`cat $data_file|grep $timezone|awk '{print $2}'`
done
prepaire_for_task

