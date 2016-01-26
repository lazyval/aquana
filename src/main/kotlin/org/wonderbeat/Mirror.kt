package org.wonderbeat

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
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Collectors
import java.util.stream.StreamSupport

private val logger = LoggerFactory.getLogger("squirtl")

data class Ticket(val reader: MonotonicConsumer, val writer: Producer, var messages: ByteBufferMessageSet? = null) {
    override fun toString() = "{${Ticket::class.java}: r: $reader, w: $writer, msg: ${messages?.size()}}"
}

data class MirrorConfig(val readBuffer: Int, val threadCountIn: Int, val threadCountOut: Int,
                        val fetchSize: Int, val connectionsMax: Int,
                        val backlog: Int, val timeoutMillis: Long = -1)


fun initConsumers(pool: ConnectionsPool<SimpleConsumer>,
                  partitionsMeta: List<PartitionMeta>,
                  fetchSize: Int = 1024 * 1024 * 7): List<MonotonicConsumer> {
    val consumerLeaders = partitionsMeta.toMap({it.partition}, {it.leader})
    logger.debug("Consumer Leaders: $consumerLeaders")
    return partitionsMeta.map {
        RetryingConsumer(MonotonicConcurrentConsumer(
                PoolAwareConsumer(it.topic, it.partition,
                        PartitionConnectionPool(pool, consumerLeaders), fetchSize),
                AtomicLong(it.endOffset - (it.endOffset - it.startOffset) / 3))) // 33% step back
    }
}

fun initProducers(pool: ConnectionsPool<SyncProducer>,
                  partitionsMeta: List<PartitionMeta>): List<Producer> {
    val producerLeaders = partitionsMeta.toMap({it.partition}, {it.leader})
    logger.debug("Producer Leaders: $producerLeaders")
    return  partitionsMeta.map {
        RetryingProducer(PoolAwareProducer(it.topic, it.partition, PartitionConnectionPool(pool,
                producerLeaders)))
    }
}

data class MirrorStatistics(val consumerPartitionStat: Map<Int, OffsetStatistics>, val messagesPerSecondTotal: Long)
data class OffsetStatistics(val startOffset: Long, val endOffset: Long)

fun run(cfg: MirrorConfig): MirrorStatistics {
    assert(partitions.size <= cfg.backlog)
    logger.debug("About to start: $cfg")
    val environment = Environment(mapOf(Pair(Environment.THREAD_POOL, ThreadPoolExecutorDispatcher(4, 4, "work-pool"))), PropertiesConfigurationReader())
    environment.setDispatcher("in-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountIn, cfg.backlog, "io-input-pool"))
    environment.setDispatcher("out-io-dispatcher", ThreadPoolExecutorDispatcher(cfg.threadCountOut, cfg.backlog, "io-output-pool"))
    val (consumerPartitionsLeaders, producerPartitionsLeaders) = StreamSupport.stream(listOf(
            { SimpleConsumer(consumersEntryPoint, kafkaPort, 9000, 1024 * 10, "squirtle-init").resolveLeaders(fromTopic) },
            { SimpleConsumer(producersEntryPoint, kafkaPort, 9000, 1024 * 10, "squirtle-init").resolveLeaders(toTopic) } )
            .toArrayList().spliterator(), true).map { it.invoke() }
            .collect(Collectors.toList()).toList() as List<Map<Int,String>>

    val producersPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
            { host ->
                val p = Properties()
                p.put("host", host)
                p.put("port", kafkaPort.toString())
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
            { host -> SimpleConsumer(host, kafkaPort, 2000, cfg.readBuffer, "squirtle-consumer") },
            { connection -> connection.close() }, {
        val poolCfg = GenericObjectPoolConfig()
        poolCfg.maxIdle = cfg.connectionsMax
        poolCfg.maxTotal = cfg.connectionsMax
        poolCfg.minIdle = cfg.connectionsMax / 2
        poolCfg
    }.invoke())
    val resolveProducerMetadataPool = ConnectionsPool(producerPartitionsLeaders.values.toSet(),
            { host -> SimpleConsumer(host, kafkaPort, 9000, 1024 * 1024 * 1, "squirtle-metadata-resolver") },
            { connection -> connection.close() })
    val (consumerPartitionsMeta, producerPartitionsMeta) = StreamSupport.stream(listOf(
                    { getPartitionsMeta(consumersPool, consumerPartitionsLeaders, fromTopic)},
                    { getPartitionsMeta(resolveProducerMetadataPool, producerPartitionsLeaders, toTopic)}).toArrayList().spliterator(), true)
                    .map { it.invoke() }
                    .collect(Collectors.toList()).toList() as List<List<PartitionMeta>>
    resolveProducerMetadataPool.close()
    val consumers = initConsumers(consumersPool, consumerPartitionsMeta, cfg.fetchSize)
    val producers = initProducers(producersPool, producerPartitionsMeta)
    val consumersFirstOffset = consumers.toMap({it.partition()}, {it.offset()})
    val skewControl = SkewController(30000, consumersFirstOffset.values.toTypedArray())

    class ReadKafka
    class WriteKafka
    val stopPromise = Promise<Event<Unit>>()

    val inIOEventBus = EventBus(environment.getDispatcher("in-io-dispatcher"), null, null, { stopPromise.onError(it) } )
    val outIOEventBus = EventBus(environment.getDispatcher("out-io-dispatcher"), null, null, { stopPromise.onError(it) })


    val readEvt = inIOEventBus.on(Selectors.`type`(ReadKafka::class.java), { input: Event<Ticket> ->
        val ticket = input.data
        input.data.messages = ticket.reader.fetch()
        outIOEventBus.notify(WriteKafka::class.java, input)
    })

    val msgCount = AtomicLong(0)
    val writeEvt = outIOEventBus.on(Selectors.`type`(WriteKafka::class.java), { input: Event<Ticket> ->
        val ticket = input.data
        val messages = ticket.messages
        if(messages != null) {
            skewControl.update(ticket.reader.partition(), ticket.reader.offset())
            if (skewControl.isSkewed()) {
                outIOEventBus.notify(WriteKafka::class.java, input)
                return
            }
            ticket.writer.write(messages)
            msgCount.addAndGet(messages.size().toLong())
        }
        ticket.messages = null
        inIOEventBus.notify(ReadKafka::class.java, input)
    })

    val startedTime = System.currentTimeMillis()
    val partitionsFitsBacklog = (cfg.backlog * 1F / partitions.size.toFloat()).toInt()
    partitionsFitsBacklog.downTo(1).forEach { i ->
        logger.debug("Submitting tickets - round $i")
        partitions.forEach { num ->
            inIOEventBus.notify(ReadKafka::class.java, Event.wrap(Ticket(
                    consumers[num],
                    producers[num]
            )))
        }
    }

    if(cfg.timeoutMillis > 0) {
        Timer().schedule(object: TimerTask() { override fun run() { stopPromise.accept(Event(Unit)) } }, cfg.timeoutMillis)
    }

    logger.debug("Awaiting termination")
    stopPromise.await(1001, TimeUnit.DAYS)
    val stoppedTime = System.currentTimeMillis();
    val finalCount = msgCount.get()
    val perMillis = finalCount.toDouble() / (stoppedTime - startedTime).toDouble()
    readEvt.cancel()
    writeEvt.cancel()
    environment.shutdown()
    consumersPool.close()
    logger.info("Wrote $finalCount messages, ${perMillis / 1000} msg per second")
    return MirrorStatistics(consumerPartitionsMeta
            .toMap ({ it.partition }, { p -> OffsetStatistics(consumersFirstOffset.get(p.partition)!!, consumers.find { it
                    .partition() == p
                    .partition }!!
                    .offset())}), (perMillis * 1000).toLong())
}


