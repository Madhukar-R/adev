#!/usr/bin/env bash

for i in 0{1..9} {11..23} ;
do

echo $i ;

bq load --source_format=AVRO spotify-user-extraction:test.165380be82c145a8b3b9107f1168566b_20180521 gs://endsong-deduplicated/events.Ap.EndSong.Deduplicated.gcs/2018-05-21T$i/*.avro

done
