package org.wonderbeat

import kafka.consumer.SimpleConsumer
import kafka.producer.SyncProducer
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

private val logger = LoggerFactory.getLogger("squirtle")

fun initConsumers(pool: ConnectionsPool<SimpleConsumer>,
                  partitionsMeta: List<PartitionMeta>,
                  fetchSize: Int = 1024 * 1024 * 7,
                  startFromOffset: (PartitionMeta) -> Long = startFromTheEnd
): List<MonotonicConsumer> {
    val consumerLeaders = partitionsMeta.associateBy({it.partition}, {it.leader})
    logger.debug("Consumer Leaders: $consumerLeaders")
    return partitionsMeta.map {
        RetryingConsumer(MonotonicConcurrentConsumer(
                PoolAwareConsumer(it.topic, it.partition,
                        PartitionConnectionPool(pool, consumerLeaders), fetchSize),
                AtomicLong(startFromOffset(it))))
    }
}

val startFromTheEnd = { meta: PartitionMeta -> meta.endOffset }
val startFromTheBeginning = { meta: PartitionMeta -> meta.endOffset }
fun startWithRollback(percentFromFront: Int): (PartitionMeta) -> Long =
        { it.endOffset - ((it.endOffset - it.startOffset) / 100) * percentFromFront }
fun startWithAvailableOffsets(offsets: Map<Int, Long>, fallback: (PartitionMeta) -> Long = startFromTheEnd): (PartitionMeta) -> Long =
        { offsets[it.partition]?:fallback(it) }


fun initProducers(pool: ConnectionsPool<SyncProducer>,
                  partitionsMeta: List<PartitionMeta>): List<Producer> {
    val producerLeaders = partitionsMeta.associateBy({it.partition}, {it.leader})
    logger.debug("Producer Leaders: $producerLeaders")
    return  partitionsMeta.map {
        RetryingProducer(PoolAwareProducer(it.topic, it.partition, PartitionConnectionPool(pool,
                producerLeaders)))
    }
}
