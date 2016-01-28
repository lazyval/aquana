package org.wonderbeat

import com.github.rholder.retry.Retryer
import com.github.rholder.retry.RetryerBuilder
import com.github.rholder.retry.StopStrategies
import kafka.api.ProducerRequest
import kafka.api.ProducerResponse
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.producer.SyncProducer
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions.asScalaMap
import java.util.concurrent.atomic.AtomicInteger

private val logger = LoggerFactory.getLogger("squirtl")


class RetryingProducer(val producer: Producer,
                       val retryer: Retryer<ProducerResponse> =
                       RetryerBuilder.newBuilder<ProducerResponse>()
                               .retryIfException()
                               .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                               .build()): Producer by producer {
    override fun write(messages: ByteBufferMessageSet): ProducerResponse =
            retryer.call { producer.write(messages) }
}

class PoolAwareProducer(val topic: String,
                        val partition: Int,
                        private val producerPool: PartitionConnectionPool<SyncProducer>): Producer {

    private val correlationId = AtomicInteger(0)

    override fun write(messages: ByteBufferMessageSet): ProducerResponse {
        val request: ProducerRequest =
                ProducerRequest(ProducerRequest.CurrentVersion(), correlationId.andDecrement, "kafka-producer",
                        1, 1000, asScalaMap(mapOf(Pair(TopicAndPartition(topic, partition), messages))))
        val connection = producerPool.borrowConnection(partition)!!
        try {
            return connection.send(request)
        } catch(ex: Exception) {
            logger.error("${this} failed to write to ${connection.config().host()}")
            throw ex
        } finally {
            producerPool.returnConnection(partition, connection)
        }
    }

    override fun toString() = "{PoolAwareProducer: [ $topic-$partition, $producerPool]}"
}

interface Producer {
    fun write(messages: ByteBufferMessageSet): ProducerResponse
}
