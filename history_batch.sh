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
	echo "submit adx task with param: "$date" "$log_range_str" "$key
	/opt/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class rtbanaly.batch.AdxBatchAnalysis  --total-executor-cores 4 --executor-cores 1 --executor-memory 3g  /home/ubuntu/spark_app/adstailor-analysis-filter.jar $date $log_range_str $key
}

prepaire_for_task(){

        while [[ $start_timestamp -lt $end_timestamp ]]; 
        do
                for key in ${!tzmap[@]};
                do
                        date=${tzmap["$key"]}
			diff_stamp=$(echo "scale=1;($key-2)*3600" | bc|awk -F '.' '{print $1}')
                	new_date=`date -d @"$(($start_timestamp+$diff_stamp))" +'%F'`
			#echo "date="$date" newdate="$new_date
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
                                campaigns=`$MYSQL -e "select count(*) from at_campaign where (activate_time >= "$activate_timestamp" and activate_time < "$expire_timestamp") or (expire_time > "$activate_timestamp" and expire_time<="$expire_timestamp")" 2>/dev/null|awk 'NR>1'`
                                if [[ $campaigns -gt 0 && ${#log_range[@]} -gt 0 ]]; then
                                        #echo "submit tasks"
                                        #echo $date" "$log_range_str" "$key
                                        submit_spark_task
                                        tzmap["$key"]="$new_date"
                                       # for i in ${!tzmap[@]};
                                       # do
                                       #         echo $i"--->"${tzmap["$i"]}
                                       # done
                                fi;
                        fi;
                done;

                start_timestamp=$((start_timestamp+30 * 60));
        done;


}

start_timestamp=$1
end_timestamp=$2
MYSQL="mysql -uadstailor -hadstailor.cz4ghpyk5xzz.ap-south-1.rds.amazonaws.com -pL1QWsUKQqTuGqFEtn5Srs adstailor_adserver"
#MYSQL="mysql -uroot -h192.168.31.15 -p123456 adstailor_adserver"
result=`$MYSQL -e "select distinct(timezone) from at_campaign" 2>/dev/null|awk 'NR>1'`
tz=$(echo $result|tr -d '\n','\t','')
declare -A tzmap=()
for timezone in $tz
do
  tzmap[$timezone]=""
done

prepaire_for_task $1 $2
echo "come into history batch process with start_timestamp="$1" and end_timestamp="$2

