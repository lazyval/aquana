# aquana
Aquana Kafka Mirror

<img src="http://cdn.bulbagarden.net/upload/f/fd/134Vaporeon.png" width="250px">


```
usage: aquana
 -backlog <arg>             [Int] Optional. Thread pool backlog.
                            Backpressure for consumer/producer stream.
                            Default: 256
 -batchSize <arg>           [Int] Optional. Consumer fetch size bytes.
                            Default: 10485760
 -connections <arg>         [Int] Optional. Max connections per host.
                            Aquana maintains connection pool for every
                            node in source/destination Kafka cluster.
                            Default: 3
 -consumer <arg>            [String] Source Kafka ip address. Any node
                            from cluster
 -consumerPort <arg>        [Int] Optional. Source Kafka port. Default:
                            9093
 -consumerTopic <arg>       [String] Source Kafka topic
 -genetics                  Genetic tests mode to find the best
                            configuration. Only 'consumer' and 'producer'
                            options required
 -help                      Show this message
 -inputPool <arg>           [Int] Optional. Consumer thread pool size.
                            Default: 10
 -outputPool <arg>          [Int] Optional. Producer thread pool size.
                            Default: 10
 -partitions <arg>          [List[Int]] - Optional. Partition numbers to
                            mirror separated by ','
 -producer <arg>            [String] Destination Kafka ip address
 -producerPort <arg>        [Int] Optional. Destination Kafka port.
                            Default: 9093
 -producerTopic <arg>       [String] Optional. Destination Kafka topic.
                            Default: consumerTopic
 -requestTimeout <arg>      [Int] Optional. The ack timeout of the
                            producer requests. Value must be non-negative
                            and non-zero. Default 10000
 -skew <arg>                [Int] Optional. Cross-partition skew factor.
                            Specifies how many batches could one partition
                            be ahead of another while mirroring. 1 - if
                            you want all partitions to be mirrored evenly.
                            Default 2
 -socketTimeout <arg>       [Int] Optional. Socket timeout milliseconds.
                            Default 9000
 -startFrom <arg>           [0|62|100] - Optional. Default: 0. Offset
                            position from the beginning (percents) mirror
                            should start from
 -startFromDetailed <arg>   [partition:offset, ...] - Optional. Offset
                            position for each partition to start from
 -tcpBuffer <arg>           [Int] Optional Tcp socket buffer. Default
                            2097152 bytes
```
