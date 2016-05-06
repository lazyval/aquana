package org.wonderbeat

import com.github.rholder.retry.Retryer
import com.github.rholder.retry.RetryerBuilder
import com.github.rholder.retry.StopStrategies
import kafka.api.ProducerRequest
import kafka.api.ProducerResponse
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.message.CompressionCodec
import kafka.message.Message
import kafka.message.`NoCompressionCodec$`
import kafka.producer.SyncProducer
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions.asScalaMap
import scala.collection.Seq
import scala.collection.mutable.`WrappedArray$`
import java.util.concurrent.atomic.AtomicInteger

private val logger = LoggerFactory.getLogger("org.wonderbeat.producers")


class RetryingProducer(val producer: Producer,
                       val retryer: Retryer<ProducerResponse> =
                       RetryerBuilder.newBuilder<ProducerResponse>()
                               .retryIfException()
                               .withRetryListener(logAttemptFailure)
                               .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                               .build()): Producer by producer {
    override fun write(serialized: ByteBufferMessageSet): ProducerResponse =
            retryer.call { producer.write(serialized) }
}

class PoolAwareProducer(val topic: String,
                        val partition: Int,
                        private val producerPool: PartitionConnectionPool<SyncProducer>): Producer {

    override fun partition(): Int = partition;

    private val correlationId = AtomicInteger(0)

    override fun write(serialized: ByteBufferMessageSet): ProducerResponse {
        val request: ProducerRequest =
                ProducerRequest(ProducerRequest.CurrentVersion(), correlationId.andDecrement, "kafka-producer",
                        1, 1000, asScalaMap(mapOf(Pair(TopicAndPartition(topic, partition), serialized))))
        val connection = producerPool.borrowConnection(partition)!!
        try {
            return connection.send(request)
        } catch(ex: Exception) {
            logger.info("${this} failed to write to ${connection.config().host()}:${connection.config().port()}")
            throw ex
        } finally {
            producerPool.returnConnection(partition, connection)
        }
    }

    override fun toString() = "{PoolAwareProducer: [ $topic-$partition, $producerPool]}"
}

class CompressingProducer(val producer: Producer, val compressionCodec: CompressionCodec): Producer by producer {
    override fun write(serialized: ByteBufferMessageSet): ProducerResponse {
        val shouldCompress = ! compressionCodec.equals(`NoCompressionCodec$`.`MODULE$`)

        val messages = if (shouldCompress) {
            // the most straightforward way is to just repack messages
            // could be optimized, but we will need to replicate and support code similar to original kafka compression
            ByteBufferMessageSet(compressionCodec, unserialize(serialized))
        } else {
            serialized
        }

        return producer.write(messages)
    }

    private fun unserialize(xs: ByteBufferMessageSet): Seq<Message> {
        val it = scala.collection.JavaConverters.asJavaIteratorConverter(xs.iterator()).asJava()
        val messages = it.asSequence().toList().map { msg -> msg.message() }
        val array = messages.toTypedArray()
        return `WrappedArray$`.`MODULE$`.make<Message>(array).toSeq()
    }
}

interface Producer {
    fun partition(): Int
    fun write(serialized: ByteBufferMessageSet): ProducerResponse
}
