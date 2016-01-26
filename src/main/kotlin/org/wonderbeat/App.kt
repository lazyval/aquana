package org.wonderbeat

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

private val logger = LoggerFactory.getLogger("squirtl")

val producersEntryPoint = "s-niagara04.nj01.303net.pvt"

val consumersEntryPoint = "p-niagara01.ami.303net.pvt"

val fromTopic = "production-input-topic"
val toTopic = "production-input-topic"
val partitions = (0 .. 89).toList()
val kafkaPort = 9093


fun main(args : Array<String>) {
    val genotype: Factory<Genotype<IntegerGene>> = Genotype.of(
            IntegerChromosome.of(1024 * 1024, 1024 * 1024 * 30, 1), // socket read buffer
            IntegerChromosome.of(15, 100, 1), // thread pool IO-read
            IntegerChromosome.of(2, 20, 1), // thread pool IO-write
            IntegerChromosome.of(1024 * 1024 * 1, 1024 * 1024 * 60, 1), // fetchSize
            IntegerChromosome.of(6, 15), // connections buffer
            IntegerChromosome.of(7, 10) // backlog 2^x
    )
    val engine = Engine.builder<IntegerGene, Long>(
            Function { run(
                    MirrorConfig(it.get(0,0).allele,
                            it.get(1,0).allele,
                            it.get(2,0).allele,
                            it.get(3,0).allele,
                            it.get(4,0).allele,
                            Math.pow(2.toDouble(), it.get(5,0).allele.toDouble()).toInt(),
                            60000)).messagesPerSecondTotal }, genotype)
            .alterers(SinglePointCrossover<IntegerGene, Long>(0.2), GaussianMutator<IntegerGene, Long>())
            .executor(Concurrency.SERIAL_EXECUTOR).build()
    val statistic = EvolutionStatistics.ofNumber<Long>()
    val file = File("result-population.xml")
    val statisticFile = File("statistics.xml")
    while(true) {
        val stream = when (file.canRead()) {
            true -> engine.stream(IO.jaxb.read(file) as Population<IntegerGene, Long>, 1)
            else -> engine.stream()
        }
        val result: EvolutionResult<IntegerGene, Long>? = stream
                .limit(10)
                .peek(statistic)
                .collect(EvolutionResult.toBestEvolutionResult());
        IO.jaxb.write(result!!.population, file)
        statisticFile.writeText(statistic.toString())
        logger.debug("Best Result $result")
    }
}


