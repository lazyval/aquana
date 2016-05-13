package org.wonderbeat

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import kafka.message.CompressionCodec
import kafka.message.`CompressionCodec$`
import kafka.message.`NoCompressionCodec$`
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.jenetics.*
import org.jenetics.engine.Engine
import org.jenetics.engine.EvolutionResult
import org.jenetics.engine.EvolutionStatistics
import org.jenetics.internal.util.Concurrency
import org.jenetics.util.Factory
import org.jenetics.util.IO
import org.slf4j.LoggerFactory
import java.io.File
import java.time.LocalDateTime
import java.util.function.Function

private val logger = LoggerFactory.getLogger("org.wonderbeat.aquana")

data class MirrorConfig(val consumerEntryPoint: HostPortTopic,
                        val producerEntryPoint: HostPortTopic,
                        val readBuffer: Int,
                        val threadCountIn: Int, val threadCountOut: Int,
                        val fetchSize: Int,
                        val connectionsMax: Int,
                        val backlog: Int,
                        val skewFactor: Int? = null,
                        val socketTimeoutMills: Int = 9000,
                        val requestTimeout: Int = 10000,
                        val onlyPartitions: List<Int>? = null,
                        val startFrom: (PartitionMeta) -> Long = startFromTheBeginning,
                        val timeoutMillis: Long = -1,
                        val compressionCodec: CompressionCodec = `NoCompressionCodec$`.`MODULE$`)


fun main(args : Array<String>) {
    val parser = DefaultParser();
    val opts = Options()
    opts.addOption(Option("genetics", "Genetic tests mode to find the best configuration. Only 'consumer' and 'producer' options required"))
    opts.addOption(Option("consumer", true, "[String] Source Kafka ip address. Any node from cluster" ))
    val defaultPort = 9093
    opts.addOption(Option("consumerPort", true, "[Int] Optional. Source Kafka port. Default: $defaultPort" ))
    opts.addOption(Option("consumerTopic", true, "[String] Source Kafka topic" ))
    opts.addOption(Option("producer", true, "[String] Destination Kafka ip address" ))
    opts.addOption(Option("producerPort", true, "[Int] Optional. Destination Kafka port. Default: $defaultPort" ))
    opts.addOption(Option("producerTopic", true, "[String] Destination Kafka topic" ))
    val defaultPoolSize = 10
    opts.addOption(Option("compressionCodec", true, "[String] Optional. Which compression codec PRODUCER should use. Default 'none'. Possible values are 'gzip' and 'snappy'"))
    opts.addOption(Option("inputPool", true, "[Int] Optional. Consumer thread pool size. Default: $defaultPoolSize" ))
    opts.addOption(Option("outputPool", true, "[Int] Optional. Producer thread pool size. Default: $defaultPoolSize" ))
    val defaultTcpBuffer = 1024 * 1024 * 2
    opts.addOption(Option("tcpBuffer", true, "[Int] Optional Tcp socket buffer. Default $defaultTcpBuffer bytes" ))
    val batchSize = 1024 * 1024 * 10
    opts.addOption(Option("batchSize", true, "[Int] Optional. Consumer fetch size bytes. Default: $batchSize" ))
    val defaultConnections = 3
    opts.addOption(Option("connections", true, "[Int] Optional. Max connections per host. Aquana maintains connection pool for every " +
            "node in source/destination Kafka cluster. Default: $defaultConnections"))
    val defaultBacklog = 256
    opts.addOption(Option("backlog", true, "[Int] Optional. Thread pool backlog. Backpressure for consumer/producer stream. Default: " +
            "$defaultBacklog"))
    val defaultSocketTimeout = 9000
    opts.addOption(Option("socketTimeout", true, "[Int] Optional. Socket timeout milliseconds. Default $defaultSocketTimeout"))
    val defaultRequestTimeout = 10000
    opts.addOption(Option("requestTimeout", true, "[Int] Optional. The ack timeout of the producer requests. Value must be non-negative and non-zero. Default $defaultRequestTimeout"))
    opts.addOption(Option("skew", true, "[Int] Optional. Cross-partition skew factor. Specifies how many batches could one partition be " +
            "ahead of another while mirroring. 1 - if you want all partitions to be mirrored evenly. Default: disabled"))
    opts.addOption(Option("partitions", true, "[List[Int]] - Optional. Partition numbers to mirror separated by ','"))
    opts.addOption(Option("startFrom", true, "[0|62|100] - Optional. Default: 0. Offset position from the beginning (percents) mirror should start from. Overrides startFromDetailed"))
    opts.addOption(Option("startFromDetailed", true, "[partition:offset, ...] - Optional. Offset position for each partition to start from"))
    opts.addOption(Option("help", false, "Show this message"))
    val options = parser.parse(opts, args);
    if(options.hasOption("help") || args.size == 1) {
        val formatter = HelpFormatter();
        formatter.printHelp( "aquana", opts);
        return;
    }
    val startFrom = when {
        options.hasOption("startFrom") -> startFrom(options.getOptionValue("startFrom").toInt())
        options.hasOption("startFromDetailed") -> {
            val option = options.getOptionValue("startFromDetailed").toString()
                    .split(' ')
                    .map { it.split(':') }
                    .associateTo(mutableMapOf<Int, Long>(), { Pair(it[0].toInt(), it[1].toLong()) })
            startWithOffsets(option)
        }
        else -> startFrom(0)
    }
    val cfg = MirrorConfig(
            consumerEntryPoint = HostPortTopic(options.getOptionValue("consumer"),
                    options.getOptionValue("consumerPort", defaultPort.toString()).toInt(),
                    options.getOptionValue("consumerTopic")
            ),
            producerEntryPoint = HostPortTopic(options.getOptionValue("producer"),
                    options.getOptionValue("producerPort", defaultPort.toString()).toInt(),
                    options.getOptionValue("producerTopic")
            ),
            readBuffer = options.getOptionValue("tcpBuffer", defaultTcpBuffer.toString()).toInt(),
            threadCountIn = options.getOptionValue("inputPool", defaultPoolSize.toString()).toInt(),
            threadCountOut = options.getOptionValue("outputPool", defaultPoolSize.toString()).toInt(),
            fetchSize = options.getOptionValue("batchSize", batchSize.toString()).toInt(),
            connectionsMax = options.getOptionValue("connections", defaultConnections.toString()).toInt(),
            backlog = options.getOptionValue("backlog", defaultBacklog.toString()).toInt(),
            socketTimeoutMills = options.getOptionValue("socketTimeout", defaultSocketTimeout.toString()).toInt(),
            requestTimeout = options.getOptionValue("requestTimeout", defaultRequestTimeout.toString()).toInt(),
            skewFactor = options.getOptionValue("skew")?.toInt(),
            onlyPartitions = options.getOptionValue("partitions")?.split(",")?.map { it.trim().toInt() },
            startFrom = startFrom,
            compressionCodec = `CompressionCodec$`.`MODULE$`.getCompressionCodec(options.getOptionValue("compressionCodec", "none"))
    )
    if(options.hasOption("genetics")) {
        logger.info("Genetic test started")
        genetics(cfg)
        return
    }
    run(cfg)
}

fun genetics(cfg: MirrorConfig) {
    val genotype: Factory<Genotype<IntegerGene>> = Genotype.of(
            IntegerChromosome.of(1024 * 1024, 1024 * 1024 * 30, 1), // socket read buffer
            IntegerChromosome.of(15, 100, 1), // thread pool IO-read
            IntegerChromosome.of(2, 20, 1), // thread pool IO-write
            IntegerChromosome.of(1024 * 1024 * 1, 1024 * 1024 * 40, 1), // fetchSize
            IntegerChromosome.of(2, 10), // connections buffer
            IntegerChromosome.of(7, 10), // backlog 2^x
            IntegerChromosome.of(1, 5) // max skew factor
    )
    val engine = Engine.builder<IntegerGene, Int>(
            Function {
                var offsetToStart = if (offsetsFile.exists()) startWithOffsets(
                        loadOffsets().checkpoints.associateBy({it.partition }, {it.offset})) else
                    startFrom(30)
                val result = run(
                        MirrorConfig(consumerEntryPoint = cfg.consumerEntryPoint,
                                producerEntryPoint = cfg.producerEntryPoint,
                                readBuffer = it.get(0, 0).allele,
                                threadCountIn = it.get(1, 0).allele,
                                threadCountOut = it.get(2, 0).allele,
                                fetchSize = it.get(3, 0).allele,
                                connectionsMax = it.get(4, 0).allele,
                                backlog = Math.pow(2.toDouble(), it.get(5, 0).allele.toDouble()).toInt(),
                                skewFactor = it.get(5,0).allele,
                                startFrom = offsetToStart,
                                timeoutMillis = 60000))
                persistOffsets(CheckPoint(result.consumerPartitionStat.toList().map { PartitionCheckpoint(it.first, it.second.endOffset) }))
                result.messagesPerSecondTotal
            }, genotype)
            .alterers(SinglePointCrossover<IntegerGene, Int>(0.2), GaussianMutator<IntegerGene, Int>())
            .executor(Concurrency.SERIAL_EXECUTOR).build()
    val statistic = EvolutionStatistics.ofNumber<Int>()
    val file = File("result-population.xml")
    val statisticFile = File("statistics.xml")
    while(true) {
        val stream = when (file.canRead()) {
            true -> engine.stream(IO.jaxb.read(file) as Population<IntegerGene, Int>, 1)
            else -> engine.stream()
        }
        val result: EvolutionResult<IntegerGene, Int>? = stream
                .limit(10)
                .peek(statistic)
                .collect(EvolutionResult.toBestEvolutionResult());
        IO.jaxb.write(result!!.population, file)
        statisticFile.writeText(statistic.toString())
        logger.debug("Best Result $result")
    }
}

val offsetsFile = File("checkpoint.save")
val mapper = ObjectMapper().registerModule(JavaTimeModule()).registerModule(KotlinModule())
data class PartitionCheckpoint(val partition: Int, val offset: Long)
data class CheckPoint(val checkpoints: List<PartitionCheckpoint>, val timestamp : LocalDateTime = LocalDateTime.now())
fun persistOffsets(checkpoint: CheckPoint) = mapper.writeValue(offsetsFile, checkpoint)
fun loadOffsets(): CheckPoint = mapper.readValue(offsetsFile, CheckPoint::class.java)
