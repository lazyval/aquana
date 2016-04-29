package org.wonderbeat

import com.google.common.base.Preconditions
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet
import kafka.network.BlockingChannel
import org.slf4j.LoggerFactory
import reactor.Environment
import reactor.bus.Event
import reactor.bus.EventBus
import reactor.bus.selector.Selectors
import reactor.core.dispatch.ThreadPoolExecutorDispatcher
import reactor.rx.Promise
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Collectors
import java.util.stream.Stream
import java.util.stream.StreamSupport

private val logger = LoggerFactory.getLogger("org.wonderbeat.mirror")

data class Ticket(val reader: MonotonicConsumer, val writer: Producer, var messages: ByteBufferMessageSet = emptyBuffer) {
    override fun toString() = "{${Ticket::class.java}: r: $reader, w: $writer, msg: ${messages.size()}}"
}

val emptyBuffer = ByteBufferMessageSet(ByteBuffer.allocate(0))

data class HostPortTopic(val host: String, val port: Int, val topic: String)
data class MirrorConfig(val consumerEntryPoint: HostPortTopic,
                        val producerEntryPoint: HostPortTopic,
                        val readBuffer: Int, val threadCountIn: Int, val threadCountOut: Int,
                        val fetchSize: Int, val connectionsMax: Int,
                        val backlog: Int, val skewFactor: Int,
                        val socketTimeoutMills: Int = 9000,
                        val requestTimeout: Int = 10000,
                        val onlyPartitions: List<Int>? = null,
                        val startFrom: (PartitionMeta) -> Long = startFromTheBeginning,
                        val timeoutMillis: Long = -1)


data class MirrorStatistics(val consumerPartitionStat: Map<Int, OffsetStatistics>, val messagesPerSecondTotal: Int)
data class OffsetStatistics(val startOffset: Long, val endOffset: Long)

fun resolveLeaders(topic: String, host: String, port: Int, socketTimeoutMills: Int,
                   bufferSize: Int = BlockingChannel.UseDefaultBufferSize(), clientId: String = "aquana-metadata-resolver"): Map<Int, HostPort>  {
    val consumer = SimpleConsumer(host, port, socketTimeoutMills, bufferSize, clientId)
    val leaders = consumer.resolveLeaders(topic)
    consumer.close()
    return leaders
}

fun <T> invokeConcurrently(vararg  functions: () -> T): Stream<T> = StreamSupport.stream(functions.toCollection(ArrayList()).spliterator(), true).map { it.invoke() }
fun <T> Stream<T>.collectToList(): List<T> = this.collect(Collectors.toList<T>()).toList()

fun run(cfg: MirrorConfig): MirrorStatistics {
    logger.debug("About to start: $cfg")
    val environment = Environment()
    environment.setDispatcher("in-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountIn, cfg.backlog, "io-input-pool"))
    environment.setDispatcher("out-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountOut, cfg.backlog, "io-output-pool"))
    val (consumerPartitionsLeaders, producerPartitionsLeaders) = invokeConcurrently(
            { resolveLeaders(cfg.consumerEntryPoint.topic, cfg.consumerEntryPoint.host, cfg.consumerEntryPoint.port, cfg.socketTimeoutMills) },
            { resolveLeaders(cfg.producerEntryPoint.topic, cfg.producerEntryPoint.host, cfg.producerEntryPoint.port, cfg.socketTimeoutMills) })
            .map { if(cfg.onlyPartitions != null) { it.filterKeys { cfg.onlyPartitions.contains(it) } } else it }
            .collectToList<Map<Int,HostPort>>()
    Preconditions.checkState(consumerPartitionsLeaders.keys.size <= cfg.backlog,
            "Backlog value [${cfg.backlog}] should be greater than partition count [${consumerPartitionsLeaders.keys.size}]")
    Preconditions.checkState(consumerPartitionsLeaders.size == producerPartitionsLeaders.size,
            "Can't mirror from ${consumerPartitionsLeaders.size} partitions to ${producerPartitionsLeaders.size} partitions. " +
                    "Count mismatch")

    val producersPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
            { hostPort -> ConnectionsPool.syncProducer(hostPort, cfg.socketTimeoutMills, cfg.requestTimeout)},
            { connection -> connection.close() },
            ConnectionsPool.genericPool(cfg.connectionsMax))
    val consumersPool = ConnectionsPool(consumerPartitionsLeaders.values.toSet(),
            { hostPort -> SimpleConsumer(hostPort.host, hostPort.port, cfg.socketTimeoutMills, cfg.readBuffer, "aquana-consumer") },
            { connection -> connection.close() },
            ConnectionsPool.genericPool(cfg.connectionsMax))
    val resolveProducerMetadataPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
            { hostPort -> SimpleConsumer(hostPort.host, hostPort.port, cfg.socketTimeoutMills, BlockingChannel.UseDefaultBufferSize(), "aquana-metadata-resolver") },
            { connection -> connection.close() })
    val (consumerPartitionsMeta, producerPartitionsMeta) = invokeConcurrently(
            { getPartitionsMeta(consumersPool, consumerPartitionsLeaders, cfg.consumerEntryPoint.topic)},
            { getPartitionsMeta(resolveProducerMetadataPool, producerPartitionsLeaders, cfg.producerEntryPoint.topic)})
            .collectToList()
    resolveProducerMetadataPool.close()
    val consumers = initConsumers(consumersPool, consumerPartitionsMeta, cfg.fetchSize, cfg.startFrom)
    val producers = initProducers(producersPool, producerPartitionsMeta)
    val offsetWeStartWith = consumers.associateBy({it.partition()}, {it.offset()})

    class ReadKafka
    class WriteKafka
    val stopPromise = Promise<Event<Unit>>()

    val inIOEventBus = EventBus(environment.getDispatcher("in-io-dispatcher"), null, null, { stopPromise.tryOnError(it) } )
    val outIOEventBus = EventBus(environment.getDispatcher("out-io-dispatcher"), null, null, { stopPromise.tryOnError(it) })

    val readEvt = inIOEventBus.on(Selectors.`type`(ReadKafka::class.java), { input: Event<Ticket> ->
        val ticket = input.data
        input.data.messages = ticket.reader.fetch()
        outIOEventBus.notify(WriteKafka::class.java, input)
    })

    val skewControl = SkewController(cfg.skewFactor, consumers.map { it.partition() })
    val msgCount = AtomicLong(0)
    val writeEvt = outIOEventBus.on(Selectors.`type`(WriteKafka::class.java), { input: Event<Ticket> ->
        val ticket = input.data
        val messages = ticket.messages
        if (!skewControl.tryAdvance(ticket.reader.partition())) {
            outIOEventBus.notify(WriteKafka::class.java, input)
        } else {
            ticket.writer.write(messages)
            msgCount.addAndGet(messages.size().toLong())
            ticket.messages = emptyBuffer
            inIOEventBus.notify(ReadKafka::class.java, input)
        }
    })

    val startedTime = System.currentTimeMillis()
    val partitionsFitsBacklog: Int = cfg.backlog / consumerPartitionsLeaders.size
    partitionsFitsBacklog.downTo(1).forEach { i ->
        logger.debug("Submitting tickets - round $i")
        consumerPartitionsLeaders.keys.forEach { num ->
            inIOEventBus.notify(ReadKafka::class.java, Event.wrap(Ticket(
                    consumers.find { it.partition() == num }!!,
                    producers.find { it.partition() == num }!!
            )))
        }
    }
    val timer = Timer()
    if(cfg.timeoutMillis > 0) {
        timer.schedule(object: TimerTask() { override fun run() { stopPromise.accept(Event(Unit)) } }, cfg.timeoutMillis)
    }

    logger.debug("Awaiting termination")
    stopPromise.await(2001, TimeUnit.DAYS)
    timer.cancel()
    val stoppedTime = System.currentTimeMillis();
    val finalCount = msgCount.get()
    val perMillis = finalCount.toDouble() / (stoppedTime - startedTime).toDouble()
    readEvt.cancel()
    writeEvt.cancel()
    environment.shutdown()
    consumersPool.close()
    logger.info("Wrote $finalCount messages, ${perMillis * 1000} msg per second")
    return MirrorStatistics(consumerPartitionsMeta
            .associateBy({ it.partition }, { p -> OffsetStatistics(offsetWeStartWith[p.partition]!!,
                    consumers.find { it.partition() == p.partition }!!.offset())}),
                    (perMillis * 1000).toInt())
}


fun <O> Promise<O>.tryOnError(error: Throwable) {
    try {
        this.onError(error)
    } catch(ex: IllegalStateException) {
        logger.trace("Promise already resolved")
    }
}
