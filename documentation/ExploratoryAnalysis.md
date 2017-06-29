# Data
* 1 month: May 2017

## Analysis Script:

## Observations:

* Most of messages are WARN/INFO: 223M/150M. ERRORs are 4.
* The number of records may have patterns by Date/ by hour of month/ or by hour of day
* Some combinations of fields may co-occur
* "msg" field has lots of unique messages, some are similar but the meaning is still unknown
* Different records (different id) belong to the same request ("req")
* The number of records per request varies, with mean = 2.6 but std = 6300
* 2 "msg" dominate. And seem to co-occur. (WARN)
* 
