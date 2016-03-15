package org.wonderbeat

import com.google.common.base.Preconditions
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet
import kafka.producer.SyncProducer
import kafka.producer.SyncProducerConfig
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import reactor.Environment
import reactor.bus.Event
import reactor.bus.EventBus
import reactor.bus.selector.Selectors
import reactor.core.config.PropertiesConfigurationReader
import reactor.core.dispatch.ThreadPoolExecutorDispatcher
import reactor.rx.Promise
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Collectors
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

fun run(cfg: MirrorConfig): MirrorStatistics {
    logger.debug("About to start: $cfg")
    val environment = Environment(mapOf(Pair(Environment.THREAD_POOL, ThreadPoolExecutorDispatcher(4, 4, "work-pool"))), PropertiesConfigurationReader())
    environment.setDispatcher("in-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountIn, cfg.backlog, "io-input-pool"))
    environment.setDispatcher("out-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountOut, cfg.backlog, "io-output-pool"))
    val (consumerPartitionsLeaders, producerPartitionsLeaders) = StreamSupport.stream(listOf(
            {
                val consumer = SimpleConsumer(cfg.consumerEntryPoint.host,
                        cfg.consumerEntryPoint.port,
                        cfg.socketTimeoutMills, 1024 * 10,
                        "aquana-init")
                val leaders = consumer.resolveLeaders(cfg.consumerEntryPoint.topic)
                consumer.close()
                leaders
            },
            {
                val consumer = SimpleConsumer(cfg.producerEntryPoint.host,
                        cfg.producerEntryPoint.port,
                        cfg.socketTimeoutMills, 1024 * 10,
                        "aquana-init")
                val leaders = consumer.resolveLeaders(cfg.producerEntryPoint.topic)
                consumer.close()
                leaders
            } )
            .toCollection(ArrayList()).spliterator(), true)
            .map { it.invoke() }
            .map { if(cfg.onlyPartitions != null) { it.filterKeys { cfg.onlyPartitions.contains(it) } } else it }
            .collect(Collectors.toList<Map<Int,HostPort>>()).toList()
    Preconditions.checkState(consumerPartitionsLeaders.keys.size <= cfg.backlog,
            "Backlog value [${cfg.backlog}] should be greater than partition count [${consumerPartitionsLeaders.keys.size}]")
    Preconditions.checkState(consumerPartitionsLeaders.size == producerPartitionsLeaders.size,
            "Can't mirror from ${consumerPartitionsLeaders.size} partitions to ${producerPartitionsLeaders.size} partitions. " +
                    "Count mismatch")

    val producersPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
            { hostPort ->
                val p = Properties()
                p.put("host", hostPort.host)
                p.put("port", hostPort.port.toString())
                p.put("socket.timeout.ms", cfg.socketTimeoutMills)
                p.put("request.timeout.ms", cfg.requestTimeout.toString())
                p.put("send.buffer.bytes", (3*1024*1024).toString() )
                SyncProducer(SyncProducerConfig(p)) },
            { connection -> connection.close() },
            {
                val poolCfg = GenericObjectPoolConfig()
                poolCfg.maxIdle = 15
                poolCfg.maxTotal = 15
                poolCfg.minIdle = 4
                poolCfg
            }.invoke())
    val consumersPool = ConnectionsPool(consumerPartitionsLeaders.values.toSet(),
            { hostPort -> SimpleConsumer(hostPort.host, hostPort.port, cfg.socketTimeoutMills, cfg.readBuffer, "aquana-consumer") },
            { connection -> connection.close() }, {
        val poolCfg = GenericObjectPoolConfig()
        poolCfg.maxIdle = cfg.connectionsMax
        poolCfg.maxTotal = cfg.connectionsMax
        poolCfg.minIdle = cfg.connectionsMax / 2
        poolCfg
    }.invoke())
    val resolveProducerMetadataPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
            { hostPort -> SimpleConsumer(hostPort.host, hostPort.port, cfg.socketTimeoutMills, 1024 * 1024 * 1, "aquana-metadata-resolver") },
            { connection -> connection.close() })
    val (consumerPartitionsMeta, producerPartitionsMeta) = StreamSupport.stream(listOf(
                    { getPartitionsMeta(consumersPool, consumerPartitionsLeaders, cfg.consumerEntryPoint.topic)},
                    { getPartitionsMeta(resolveProducerMetadataPool, producerPartitionsLeaders, cfg.producerEntryPoint.topic)})
            .toCollection(ArrayList()).spliterator(), true)
            .map { it.invoke() }
            .collect(Collectors.toList<List<PartitionMeta>>()).toList()
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
    stopPromise.await(1001, TimeUnit.DAYS)
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
