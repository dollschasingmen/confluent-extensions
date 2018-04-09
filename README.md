# confluent-extensions

A collection of extensions for the Confluent Platform

Caution: this is pretty nascent at the moment, feel free to contribute or fork!

## Kafka Connect

### Transforms

#### InsertRuntimeField

This calculates a run time given a start and end time.

Must be unix timestamps (ie, we simply operate on `Long`s)


| Configuration  | Type   | Required|Description | 
|----------------|--------|----------|------------|
| field.start  | String | Y |Field name of the start timestamp
| field.end| String | N |Field name of the end timestamp, defaults to wall clock if not supplied|
| field.runtime | String | N |Field name of the resultant runtime duration. Defaults to "runtime"



#### InsertWallclockTimestampField

This inserts a wall clock (at the time of transform) in the record for a specified field name.

| Configuration  | Type   | Required|Description | 
|----------------|--------|----------|------------|
| timestamp.field  | String | Y |Field name of the wall clock timestamp|

