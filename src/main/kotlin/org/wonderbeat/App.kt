package org.wonderbeat

import com.github.rholder.retry.RetryerBuilder
import com.github.rholder.retry.StopStrategies
import org.apache.commons.cli.DefaultParser
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
import java.util.function.Function
import javax.xml.bind.JAXB

private val logger = LoggerFactory.getLogger("squirtle")

val offsetsFile = File("offsets.save")

fun main(args : Array<String>) {
    val parser = DefaultParser();
    val opts = Options()
    opts.addOption(Option("genetics", "run genetics tests" ))
    opts.addOption(Option("consumer", true, "consumer entry point" ))
    opts.addOption(Option("consumer-port", true, "consumer port" ))
    opts.addOption(Option("consumer-topic", true, "consumer topic" ))
    opts.addOption(Option("producer", true, "producer entry point" ))
    opts.addOption(Option("producer-port", true, "producer port" ))
    opts.addOption(Option("producer-topic", true, "producer topic" ))
    opts.addOption(Option("input", true, "input thread pool size" ))
    opts.addOption(Option("output", true, "output thread pool size" ))
    opts.addOption(Option("buffer", true, "fetch size bytes" ))
    opts.addOption(Option("connections", true, "max connections per host" ))
    opts.addOption(Option("backlog", true, "thread pool backlog" ))
    opts.addOption(Option("skew", true, "cross-partition skew factor" ))
    val options = parser.parse(opts, args);
    val cfg = MirrorConfig(
            HostPortTopic(options.getOptionValue("consumer"),
                    options.getOptionValue("consumerPort", "9093").toInt(),
                    options.getOptionValue("consumerTopic", "production-input-topic")
            ),
            HostPortTopic(options.getOptionValue("producer"),
                    options.getOptionValue("producerPort", "9093").toInt(),
                    options.getOptionValue("producerTopic", "production-input-topic")
            ),
            options.getOptionValue("buffer", (1024 * 1024 * 10).toString()).toInt(),
            options.getOptionValue("input", 20.toString()).toInt(),
            options.getOptionValue("output", 20.toString()).toInt(),
            options.getOptionValue("buffer", (1024 * 1024 * 10).toString()).toInt(),
            options.getOptionValue("connections", 3.toString()).toInt(),
            options.getOptionValue("backlog", 256.toString()).toInt(),
            options.getOptionValue("skew", 2.toString()).toInt()
    )
    val retry = RetryerBuilder.newBuilder<Unit>().retryIfException().withStopStrategy(StopStrategies.neverStop()).build()
    if(options.hasOption("genetics")) {
        retry.call { genetics(cfg) }
        return
    }
    retry.call {
        run(cfg)
    }
}

fun genetics(cfg: MirrorConfig) {
    logger.info("Genetic test started")
    val genotype: Factory<Genotype<IntegerGene>> = Genotype.of(
            IntegerChromosome.of(1024 * 1024, 1024 * 1024 * 30, 1), // socket read buffer
            IntegerChromosome.of(15, 100, 1), // thread pool IO-read
            IntegerChromosome.of(2, 20, 1), // thread pool IO-write
            IntegerChromosome.of(1024 * 1024 * 1, 1024 * 1024 * 60, 1), // fetchSize
            IntegerChromosome.of(2, 7), // connections buffer
            IntegerChromosome.of(7, 10), // backlog 2^x
            IntegerChromosome.of(1, 10) // max skew factor
    )
    val engine = Engine.builder<IntegerGene, Int>(
            Function {
                var offsetToStart = if (offsetsFile.exists()) startWithAvailableOffsets(loadOffsets()) else startWithRollback(30)
                val result = run(
                        MirrorConfig(cfg.consumerEntryPoint,
                                cfg.producerEntryPoint,
                                it.get(0, 0).allele,
                                it.get(1, 0).allele,
                                it.get(2, 0).allele,
                                it.get(3, 0).allele,
                                it.get(4, 0).allele,
                                Math.pow(2.toDouble(), it.get(5, 0).allele.toDouble()).toInt(),
                                it.get(5,0).allele,
                                offsetToStart,
                                120000))
                //   persistOffsets(result.consumerPartitionStat)
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

fun persistOffsets(offsetStats: Map<Int, OffsetStatistics>) = JAXB.marshal(offsetStats.mapValues { it.value.endOffset }, offsetsFile)
fun loadOffsets() = JAXB.unmarshal(offsetsFile, Map::class.java) as Map<Int, Long>
