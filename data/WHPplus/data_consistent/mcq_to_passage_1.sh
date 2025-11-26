#!/bin/bash

NUM_GPUS=4
NUM_SHARDS=12

for (( i=0; i<$NUM_GPUS; i++ ))
do
   echo "Launching shard $i on device $i..."
   # Run in background with nohup so it survives terminal closure
   # We log stdout/stderr to a specific log file per shard
   nohup python mcq_to_passage.py \
     --num_shards $NUM_SHARDS \
     --shard_id $i \
     --device_id $i \
     > logs_shard_$i.txt 2>&1 &
done

echo "All processes launched."