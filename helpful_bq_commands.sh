#!/usr/bin/env bash

# get total slot milli seconds
bq --format prettyjson show -j job_NhDAoK2yeJFIEP9UoqE5MzRvFdCx| jq '.statistics["query"]["timeline"]'|jq '.[]|select(.pendingUnits=="0")'|jq '.totalSlotMs|tonumber'

cat bqjobdetails2.json | jq '.statistics["query"]["timeline"]'|jq '.[]|select(.pendingUnits=="0")'|jq '.totalSlotMs|tonumber'

# get job details
