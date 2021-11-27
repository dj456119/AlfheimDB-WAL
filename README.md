<!--
 * @Descripttion: 
 * @version: 
 * @Author: cm.d
 * @Date: 2021-11-20 11:18:45
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-27 13:00:45
-->

# AlfheimDB-WAL

The AlfheimDB's high performance write-ahead log.

# Example

[AlfheimDB-WAL-Example](https://github.com/dj456119/AlfheimDB-WAL-Example)

# Core Struct
````
 WAL file struct in storage:  
 ┌───────────┬───────────┐  
 │ 1K header │   logs    │  
 └───────────┴───────────┘  
 The file header struct:  
 ┌───────────────┬─────────────────────────────┐  
 │ Length 8Bytes │             Data            │  
 └───────────────┴─────────────────────────────┘  
 The log item struct:  
 ┌───────────────┬──────────────┬─────────────────────────────────┐  
 │ Length 8Bytes │ Index 8Bytes │              Data               │  
 └───────────────┴──────────────┴─────────────────────────────────┘  
 ````
# Truncate Func

## Case 1
````
 The log min index is 5, max index is 13
 If start in (-,5] && end in [13,-)
 Need truncate all log, so we remove this file
 ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
 │5│6│7│8│9│10│11│12│13│
 └─┴─┴─┴─┴─┴──┴──┴──┴──┘
````
## Case 2
````
 The log min index is 5, max index is 13
 If start in [5,13) && end in [13,-)
 Need truncate from start to last log
 ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
 │5│6│7│8│9│10│11│12│13│
 └─┴─┴─┴─┴─┴──┴──┴──┴──┘
```` 
## Case 3
````
 The log min index is 5, max index is 13
 If start in (-,5] && end in [5,13]
 Need truncate from the first log to end
 Put these pos into TruncateArea
 ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
 │5│6│7│8│9│10│11│12│13│
 └─┴─┴─┴─┴─┴──┴──┴──┴──┘
````
## Case 4
````
 The log min index is 5, max index is 13
 If start in [5,13] && end in [5,13]
 Need truncate from start to end
 Put these pos into TruncateArea
 ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
 │5│6│7│8│9│10│11│12│13│
 └─┴─┴─┴─┴─┴──┴──┴──┴──┘
````
# Benchmarks

## MACBOOK PRO 2020 M1 SSD

BatchWrite: 100bytes, 200 logs, loop 10000 1.6s 

## CENTOS 7 8C8G HDD

BatchWrite: 100bytes, 200 logs, loop 10000 5.6s 