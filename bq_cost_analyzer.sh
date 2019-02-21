#!/usr/bin/env bash

<<Description
This script calculate the bq jobs total bq slot time and cost.
Script expects file with all the bq Ids

jq tool must be installed on the Environment
Description

# all job ids for on a file
Level2_jobids_file='../Input/Level2_bq_jobids'

declare -i total_number_of_slots=25000
declare -i total_flat_bq_cost=330000
#$(( 25000/2000 * 45000)) = 562500 -- If cost per 2000 slots is 45000

declare -i number_days_in_month=30
declare -i number_ms_in_sec=1000
declare -i number_sec_in_min=60
declare -i number_min_in_hour=60
declare -i number_hour_in_day=24
declare -i total_hours_per_month=$(($number_days_in_month*$number_hour_in_day))
declare -i total_mins_per_month=$(( $total_hours_per_month*$number_min_in_hour))
declare -i total_secs_per_month=$(( $total_mins_per_month*$number_sec_in_min ))
declare -i total_ms_per_month=$(( $total_secs_per_month*$number_ms_in_sec ))

declare -i total_slot_days_per_month=$(($number_days_in_month*total_number_of_slots))
declare -i total_slot_hours_per_month=$(($total_hours_per_month*total_number_of_slots))
declare -i total_slot_mins_per_month=$(( $total_mins_per_month*total_number_of_slots))
declare -i total_slot_secs_per_month=$(( $total_secs_per_month*total_number_of_slots ))
declare -i total_slot_ms_per_month=$(( total_ms_per_month*total_number_of_slots ))

declare -i floating_point_scale=15
declare slot_cost_per_ms=`echo "scale=$floating_point_scale;$total_flat_bq_cost/$total_slot_ms_per_month"|bc`
declare slot_cost_per_sec=`echo "scale=$floating_point_scale;$total_flat_bq_cost/$total_slot_secs_per_month"|bc`
declare slot_cost_per_min=`echo "scale=$floating_point_scale;$total_flat_bq_cost/$total_slot_mins_per_month"|bc`
declare slot_cost_per_hour=`echo "scale=$floating_point_scale;$total_flat_bq_cost/$total_slot_hours_per_month"|bc`
declare slot_cost_per_day=`echo "scale=$floating_point_scale;$total_flat_bq_cost/$total_slot_days_per_month"|bc`
#echo $slot_cost_per_ms, $slot_cost_per_sec
#echo "slot cost per day:"$slot_cost_per_day
#echo "slot cost per hour:"$slot_cost_per_hour
#echo "slot cost per minute:"$slot_cost_per_min

declare -i all_jobs_total_slot_ms=0
declare all_jobs_total_slot_secs
declare all_jobs_total_slot_mins
declare all_jobs_total_slot_hours
declare all_jobs_total_slot_days

declare -i job_total_slot_ms=0
declare job_total_slot_secs

#test variable
declare all_jobs_total_secs=0

echo start
echo "------------- Time taken by individual bq jobs, cost.Start ----------------"

while read jobid;
do

#job_total_slot_ms=$(bq --format prettyjson show -j job_-StyvC6tJsY0pMcbNL1QR5pVLV1f| jq '.statistics["query"]["timeline"]'|jq '.[]|select(.pendingUnits=="0")'|echo $(jq '.totalSlotMs|tonumber'))
job_total_slot_ms=$(bq --format prettyjson show -j $jobid| jq '.statistics["query"]["timeline"]'|jq '.[]|select(.pendingUnits=="0")'|jq --slurp '.'|echo $(jq '.[0]["totalSlotMs"]|tonumber'))

#echo job_total_slot_ms

#Test value
#job_total_slot_ms=$(echo "scale=$floating_point_scale;1217176713" | bc)

all_jobs_total_slot_ms=$(( $all_jobs_total_slot_ms+$job_total_slot_ms ))

job_total_slot_secs=`echo "scale=$floating_point_scale;$job_total_slot_ms/$number_ms_in_sec"|bc`
job_total_slot_mins=`echo "scale=$floating_point_scale;$job_total_slot_ms/$number_sec_in_min"|bc`
job_total_slot_hourss=`echo "scale=$floating_point_scale;$job_total_slot_ms/$number_min_in_hour"|bc`

all_jobs_total_secs=`echo "scale=$floating_point_scale;$all_jobs_total_secs+$job_total_slot_secs"|bc`

echo "Job ID: "$jobid  ", totalSlotMs:" $job_total_slot_ms ", totalSlotSecs:"$job_total_slot_secs", totalSlotMins:"$job_total_slot_secs", totalSlotHours:"$job_total_slot_secs", Cost:" `echo "scale=$floating_point_scale;$job_total_slot_ms*$slot_cost_per_ms"|bc`
# $job_total_slot_secs, #$all_jobs_total_slot_ms, $all_jobs_total_secs

done < $Level2_jobids_file

all_jobs_total_slot_secs=`echo "scale=$floating_point_scale;$all_jobs_total_slot_ms/$number_ms_in_sec"|bc`
all_jobs_total_slot_mins=`echo "scale=$floating_point_scale;$all_jobs_total_slot_secs/$number_sec_in_min"|bc`
all_jobs_total_slot_hours=`echo "scale=$floating_point_scale;$all_jobs_total_slot_mins/$number_min_in_hour"|bc`
all_jobs_total_slot_days=`echo "scale=$floating_point_scale;$all_jobs_total_slot_hours/$number_hour_in_day"|bc`
echo "------------- Time taken by individual bq jobs, cost. End ----------------"

echo "------------- Summary -- All BQ jobs Cost Analysis Start -----------------"
echo "Total slots available for Spotify" `echo "scale=$floating_point_scale;$total_number_of_slots * $number_days_in_month"|bc`
echo "Total slots time used in milli seconds:"$all_jobs_total_slot_ms", in seconds:"$all_jobs_total_slot_secs", in Minutes:"$all_jobs_total_slot_mins", in hours:"$all_jobs_total_slot_hours", in days:"$all_jobs_total_slot_days
echo "Slot cost - per milli second:"$slot_cost_per_ms", per second:"$slot_cost_per_sec", per Minute:"$slot_cost_per_min ", per hour:"$slot_cost_per_hour", per day:"$slot_cost_per_day
echo "Total cost for all the jobs together: "`echo "scale=$floating_point_scale;$all_jobs_total_slot_ms * $slot_cost_per_ms"|bc`

echo "------------- All BQ jobs Cost Analysis End -----------------"

echo end
