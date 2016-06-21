package org.wonderbeat

import com.github.rholder.retry.Retryer
import com.github.rholder.retry.RetryerBuilder
import com.github.rholder.retry.StopStrategies
import com.github.rholder.retry.WaitStrategies
import kafka.api.FetchRequestBuilder
import kafka.common.ErrorMapping
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = LoggerFactory.getLogger("org.wonderbeat.consumers")

interface MonotonicConsumer {
    fun fetch(): ByteBufferMessageSet
    fun offset(): Long
}

class RetryingConsumer(private val delegate: MonotonicConsumer,
                       private val retryer: Retryer<ByteBufferMessageSet> = RetryerBuilder.newBuilder<ByteBufferMessageSet>()
                                            .retryIfException()
                                            .withRetryListener(logAttemptFailure)
                                            .withWaitStrategy(WaitStrategies.exponentialWait(2, 10, TimeUnit.SECONDS))
                                            .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                                            .build()): MonotonicConsumer by delegate {
    override fun fetch(): ByteBufferMessageSet = retryer.call {  delegate.fetch() }
}

class MonotonicConcurrentConsumer(private val consumer: PoolAwareConsumer,
                                  private var offset: AtomicLong): MonotonicConsumer {
    private data class MessagesFromOffset(val messageSet: ByteBufferMessageSet, val offset: Long)

    override fun fetch(): ByteBufferMessageSet =
        generateSequence {
            val ofst = offset.get()
            val messages = consumer.fetch(ofst)
            Pair(messages, ofst)
        }.filter {
            it.first != null && it.first!!.size() > 0
        }.map {
            MessagesFromOffset(it.first!!, it.second)
        }.filter {
            val inTime = offset.compareAndSet(it.offset, it.offset + it.messageSet.size())
            if(!inTime && logger.isInfoEnabled) {
                logger.info("Fetch took too long for $consumer and offset ${it.offset}")
            }
            inTime
        }.first().messageSet

    override fun offset(): Long = offset.get()
}

class PoolAwareConsumer(val topic: String,
                        val partition: Int,
                        val consumersPool: PartitionConnectionPool<SimpleConsumer>,
                        val fetchSize: Int = 1024 * 1024 * 7,
                        maxWaitMs: Int = 2000,
                        clientId: String = "aquana",
                        minBytes: Int = fetchSize / 2) {

    private val fetchBuilder = FetchRequestBuilder().clientId(clientId).maxWait(maxWaitMs).minBytes(minBytes)

    fun fetch(offset: Long): ByteBufferMessageSet? {
        val connection = consumersPool.borrowConnection(partition)!!
        val request = fetchBuilder.addFetch(topic, partition, offset, fetchSize).build()
        try {
            val response = connection.fetch(request)
            if(response.hasError()) {
                throw ErrorMapping.exceptionFor(response.errorCode(topic, partition))
            }
            return response.messageSet(topic, partition)
        } finally {
            consumersPool.returnConnection(partition, connection)
        }
    }

    override fun toString() = "{${PoolAwareConsumer::class.java} [$topic-$partition, $fetchSize bytes]}"
}
