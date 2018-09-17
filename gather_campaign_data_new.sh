#!/bin/bash
start_date=$1
end_date=$2
campaign_id=$3
logtype=$4
filename=$5
beg_s=`date -d "$start_date" +%s`
end_s=`date -d "$end_date" +%s`
grep_data(){
	gunzip atd00*/*.gz 
 #       cat atd00*/*|grep '"cid":"'$campaign_id'"'|awk -F "ip" '{gaid=substr($2, 4, index($2, "devmk")-7); print gaid}'>> gaid_$campaign_id.log
        cat atd00*/*|grep '"cid":"'$campaign_id'"'|grep '"adid":"2335"'|awk -F "did" '{gaid=substr($2, 4, index($2, "devmk")-7);print gaid}'>> gaid_$campaign_id.log
 #        cat atd00*/*|grep $campaign_id >> "$filename".log
 #	cat atd00*/* |awk -F '"tz":' '{print substr($2, 0, index($2, ",")-1)" "substr($2, index($2,"pixelts")+9, 10)}' >> timezone.log
	 rm -rf atd00*/*
}
rm -rf atd00*/*
while [ "$beg_s" -le "$end_s" ]
        do
	echo $beg_s
        date_s=$(date -d @$beg_s +"%Y%m%d%H")
        echo "fetch log with date = "$date_s"*"
	s3cmd get s3://adstailor/xdsp/log/kafka/atd001/ad-"$logtype"-"$date_s"* atd001/
        grep_data
	s3cmd get s3://adstailor/xdsp/log/kafka/atd002/ad-"$logtype"-"$date_s"* atd002/
        grep_data
	s3cmd get s3://adstailor/xdsp/log/kafka/atd003/ad-"$logtype"-"$date_s"* atd003/
        grep_data
	beg_s=$((beg_s+3600))
done
