package org.wonderbeat

import com.google.common.base.Preconditions
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet
import kafka.network.BlockingChannel
import org.slf4j.LoggerFactory
import reactor.Environment
import reactor.bus.Event
import reactor.bus.EventBus
import reactor.bus.selector.Selectors.`$`
import reactor.core.Dispatcher
import reactor.core.dispatch.ThreadPoolExecutorDispatcher
import reactor.rx.Promise
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = LoggerFactory.getLogger("org.wonderbeat.mirror")

data class Ticket(val taskId: Int, val reader: MonotonicConsumer, val writer: Producer, var messages: ByteBufferMessageSet = emptyBuffer) {
    override fun toString() = "{${Ticket::class.java}: r: $reader, w: $writer, msg: ${messages.size()}}"
}
data class HostPortTopic(val host: String, val port: Int, val topic: String)
data class MirrorStatistics(val consumerPartitionStat: Map<Int, OffsetStatistics>, val messagesPerSecondTotal: Int)
data class OffsetStatistics(val startOffset: Long, val endOffset: Long)
val emptyBuffer = ByteBufferMessageSet(ByteBuffer.allocate(0))

data class Dispatchers(val input: Dispatcher, val output: Dispatcher)
fun initDispatchers(env: Environment, cfg: MirrorConfig): Dispatchers {
    env.setDispatcher("in-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountIn, cfg.backlog, "io-input-pool"))
    env.setDispatcher("out-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountOut, cfg.backlog, "io-output-pool"))
    return Dispatchers(env.getDispatcher("in-io-dispatcher"), env.getDispatcher("out-io-dispatcher"))
}

object ReadKafka
object WriteKafka

fun run(cfg: MirrorConfig): MirrorStatistics {
    logger.debug("About to start: $cfg")
    val (consumerPartitionsLeaders, producerPartitionsLeaders) =
            invokeConcurrently({ resolveLeaders(cfg.consumerEntryPoint) }, { resolveLeaders(cfg.producerEntryPoint) })
            .map { if(cfg.onlyPartitions != null) { it.filterKeys { cfg.onlyPartitions.contains(it) } } else it }
            .collectToList<Map<Int,HostPort>>()
    Preconditions.checkState(consumerPartitionsLeaders.keys.size <= cfg.backlog,
            "Backlog value [${cfg.backlog}] should be greater than partition count [${consumerPartitionsLeaders.keys.size}]")
    Preconditions.checkState(consumerPartitionsLeaders.size == producerPartitionsLeaders.size,
            "Can't mirror from ${consumerPartitionsLeaders.size} partitions to ${producerPartitionsLeaders.size} partitions. " +
                    "Count mismatch")

    val consumersPool = ConnectionsPool(consumerPartitionsLeaders.values.toSet(),
            { hostPort -> SimpleConsumer(hostPort.host, hostPort.port, cfg.socketTimeoutMills, cfg.readBuffer, "aquana-consumer") },
            { it.close() },
            ConnectionsPool.genericPool(cfg.connectionsMax))

    val producersPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
            { hostPort -> ConnectionsPool.syncProducer(hostPort, cfg.socketTimeoutMills, cfg.requestTimeout, cfg.compressionCodec)},
            { it.close() },
            ConnectionsPool.genericPool(cfg.connectionsMax))
    val (consumerPartitionsMeta, producerPartitionsMeta) = invokeConcurrently(
            { getPartitionsMeta(consumersPool, consumerPartitionsLeaders, cfg.consumerEntryPoint.topic)},
            {
                val resolveProducerMetadataPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
                    { hostPort -> SimpleConsumer(hostPort.host, hostPort.port, cfg.socketTimeoutMills, BlockingChannel.UseDefaultBufferSize(), "aquana-metadata-resolver") },
                    { it.close() })
                getPartitionsMeta(resolveProducerMetadataPool, producerPartitionsLeaders, cfg.producerEntryPoint.topic).apply {
                    resolveProducerMetadataPool.close()
                }
            }).collectToList()

    val producers = initProducers(producersPool, producerPartitionsMeta)
    val consumers = initConsumers(consumersPool, consumerPartitionsMeta, cfg.fetchSize, cfg.startFrom)
    val offsetWeStartWith = consumers.mapValues { it.value.offset() }

    val stopPromise = Promise<Event<Unit>>()
    val environment = Environment()
    val dispatchers = initDispatchers(environment, cfg)
    val inIOEventBus = EventBus(dispatchers.input, null, null, { stopPromise.tryOnError(it) } )
    val outIOEventBus = EventBus(dispatchers.output, null, null, { stopPromise.tryOnError(it) })
    val skewControl: SkewControl = when (cfg.skewFactor) {
        null -> NoopSkewControl
        else -> ConcurrentSkewControl(cfg.skewFactor, consumers.keys.toList())
    }
    val msgCount = AtomicLong()

    val readEvt = inIOEventBus.on(`$`(ReadKafka), { input: Event<Ticket> ->
        input.data.messages = input.data.reader.fetch()
        outIOEventBus.notify(WriteKafka, input)
    })
    val writeEvt = outIOEventBus.on(`$`(WriteKafka), { input: Event<Ticket> ->
        val ticket = input.data
        if (!skewControl.tryAdvance(ticket.taskId)) {
            outIOEventBus.notify(WriteKafka, input)
        } else {
            val messages = ticket.messages
            ticket.writer.write(messages)
            ticket.messages = emptyBuffer
            inIOEventBus.notify(ReadKafka, input)
            msgCount.addAndGet(messages.size().toLong())
        }
    })

    val startedTime = System.currentTimeMillis()
    val partitionsFitsBacklog: Int = cfg.backlog / consumerPartitionsLeaders.size
    (1 .. partitionsFitsBacklog).forEach { i ->
        logger.debug("Submitting tickets - round $i")
        consumerPartitionsLeaders.keys.forEach { num ->
            inIOEventBus.notify(ReadKafka, Event.wrap(Ticket(num, consumers[num]!!, producers[num]!!)))
        }
    }
    val timer = Timer()
    if(cfg.timeoutMillis > 0) {
        timer.schedule(object: TimerTask() { override fun run() { stopPromise.accept(Event(Unit)) } }, cfg.timeoutMillis)
    }
    logger.debug("Awaiting termination")
    stopPromise.await(2001, TimeUnit.DAYS)
    timer.cancel()
    readEvt.cancel()
    writeEvt.cancel()
    environment.shutdown()
    consumersPool.close()
    val finalCount = msgCount.get()
    val stoppedTime = System.currentTimeMillis();
    val perMillis = finalCount.toDouble() / (stoppedTime - startedTime).toDouble()
    logger.info("Wrote $finalCount messages, ${perMillis * 1000} msg per second")
    return MirrorStatistics(consumerPartitionsMeta
            .associateBy({ it.partition }, { p -> OffsetStatistics(offsetWeStartWith[p.partition]!!,
                    consumers[p.partition]!!.offset())}),
                    (perMillis * 1000).toInt())
}


fun <O> Promise<O>.tryOnError(error: Throwable) {
    try {
        this.onError(error)
    } catch(ex: IllegalStateException) {
        logger.trace("Promise already resolved")
    }
}
