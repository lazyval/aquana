package org.wonderbeat

import kafka.consumer.SimpleConsumer
import kafka.message.CompressionCodec
import kafka.network.BlockingChannel
import kafka.producer.SyncProducer
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

private val logger = LoggerFactory.getLogger("org.wonderbeat.connections")

fun initConsumers(pool: ConnectionsPool<SimpleConsumer>,
                  partitionsMeta: List<PartitionMeta>,
                  fetchSize: Int = 1024 * 1024 * 7,
                  startFromOffset: (PartitionMeta) -> Long = startFromTheEnd): Map<Int, MonotonicConsumer> {
    val consumerLeaders = partitionsMeta.associateBy({it.partition}, {it.leader})
    logger.debug("Consumer Leaders: $consumerLeaders")
    return partitionsMeta.associateBy({ it.partition } ,
            {
                RetryingConsumer(MonotonicConcurrentConsumer(
                        PoolAwareConsumer(it.topic, it.partition,
                                PartitionConnectionPool(pool, consumerLeaders), fetchSize),
                        AtomicLong(startFromOffset(it))))
            })
}

val startFromTheEnd = { meta: PartitionMeta -> meta.endOffset }
val startFromTheBeginning = { meta: PartitionMeta -> meta.startOffset }
fun startFrom(percentFromBeginning: Int): (PartitionMeta) -> Long =
        { it.startOffset + ((it.endOffset - it.startOffset) / 100) * percentFromBeginning }

class NoOffsetToStartForPartition(msg: String?): RuntimeException(msg)
fun startWithOffsets(offsets: Map<Int, Long>,
                     fallback: (PartitionMeta) -> Long =
                     { throw NoOffsetToStartForPartition("No offset to start for $it")  }): (PartitionMeta) -> Long =
        { offsets[it.partition]?:fallback(it) }


fun initProducers(pool: ConnectionsPool<SyncProducer>,
                  partitionsMeta: List<PartitionMeta>,
                  compressionCodec: CompressionCodec): Map<Int, Producer> {
    val producerLeaders = partitionsMeta.associateBy({it.partition}, {it.leader})
    logger.debug("Producer Leaders: $producerLeaders")
    return  partitionsMeta.associateBy({ it.partition }, {
        CompressingProducer(
            RetryingProducer(
                PoolAwareProducer(it.topic, it.partition, PartitionConnectionPool(pool,producerLeaders))
            ),
            compressionCodec
        )
    })
}

fun resolveLeaders(hostPortTopic: HostPortTopic, socketTimeoutMills: Int = 10000,
                   bufferSize: Int = BlockingChannel.UseDefaultBufferSize(), clientId: String = "aquana-metadata-resolver"): Map<Int, HostPort>  {
    val consumer = SimpleConsumer(hostPortTopic.host, hostPortTopic.port, socketTimeoutMills, bufferSize, clientId)
    val leaders = consumer.resolveLeaders(hostPortTopic.topic)
    consumer.close()
    return leaders
}
