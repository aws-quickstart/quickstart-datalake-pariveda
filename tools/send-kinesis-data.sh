#!/bin/sh

streamName=$1
aws kinesis put-record --stream-name $streamName --data file://tools/kinesisData.json --partition-key "key"