package org.wonderbeat

import com.github.rholder.retry.Retryer
import com.github.rholder.retry.RetryerBuilder
import com.github.rholder.retry.StopStrategies
import kafka.api.FetchRequestBuilder
import kafka.common.ErrorMapping
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

private val logger = LoggerFactory.getLogger("org.wonderbeat.consumers")

class RetryingConsumer(private val delegate: MonotonicConsumer,
                       val retryer: Retryer<ByteBufferMessageSet> = RetryerBuilder.newBuilder<ByteBufferMessageSet>()
                                            .retryIfException()
                                            .withRetryListener(logAttemptFailure)
                                            .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                                            .build()): MonotonicConsumer by delegate {
    override fun fetch(): ByteBufferMessageSet = retryer.call {  delegate.fetch() }
}

class MonotonicConcurrentConsumer(val consumer: PoolAwareConsumer, var offset: AtomicLong): MonotonicConsumer {

    override fun fetch(): ByteBufferMessageSet {
        var messages: ByteBufferMessageSet?
        do {
            val ofst = offset.get()
            messages = consumer.fetch(ofst)
            val size = if(messages == null) 0 else messages.size()
            val inTime = offset.compareAndSet(ofst, ofst + size)
            if(!inTime && logger.isInfoEnabled) {
                logger.info("Fetch took too long for $consumer")
            }
        } while(messages == null || !inTime)
        return messages
    }

    override fun offset(): Long = offset.get()
    override fun partition(): Int = consumer.partition

}

interface MonotonicConsumer {
    fun fetch(): ByteBufferMessageSet
    fun offset(): Long
    fun partition(): Int
}

class PoolAwareConsumer(val topic: String,
                        val partition: Int,
                        val consumersPool: PartitionConnectionPool<SimpleConsumer>,
                        val fetchSize: Int = 1024 * 1024 * 7,
                        val maxWaitMs: Int = 2000,
                        val minBytes: Int = fetchSize / 2) {

    fun fetch(offset: Long): ByteBufferMessageSet? {
        val request = FetchRequestBuilder().addFetch(topic, partition, offset, fetchSize)
                .maxWait(maxWaitMs)
                .minBytes(minBytes)
                .build()
        val connection = consumersPool.borrowConnection(partition)!!
        try {
            val response = connection.fetch(request)
            if(response.hasError()) {
                throw ErrorMapping.exceptionFor(response.errorCode(topic, partition))
            } else {
                val messages = response.messageSet(topic, partition)
                return messages
            }
        } finally {
            consumersPool.returnConnection(partition, connection)
        }
    }

    override fun toString() = "{${PoolAwareConsumer::class.java} [$topic-$partition, $fetchSize bytes]}"
}
