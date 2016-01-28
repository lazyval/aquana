package org.wonderbeat

import com.google.common.util.concurrent.RateLimiter
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

class SkewController(val maxSkew: Int, val bucketsSize: Int) {

    private val logger = LoggerFactory.getLogger(SkewController::class.java)

    private val rateLimiter = RateLimiter.create(0.05)

    private val buckets = bucketsSize.downTo(1).map { AtomicLong(0) }

    fun update(bucket: Int) = buckets[bucket].incrementAndGet()

    fun isSkewed(): Boolean {
        val minMax = buckets.fold(Pair(buckets.first().get(), buckets.first().get()), { acc, item ->
            val itemVal = item.get()
            Pair(if (acc.first < itemVal) acc.first else itemVal, if(acc.second > itemVal) acc.second else itemVal )
        })
        val skew = minMax.second - minMax.first > maxSkew
        if(skew && logger.isDebugEnabled && rateLimiter.tryAcquire()) {
            logSkewState(minMax.first)
        }
        return skew
    }

    private fun logSkewState(minBucketVal: Long) {
        val statStrings = printVertically(buckets.mapIndexed { i, value ->
            val skewSize = (value.get() - minBucketVal).toInt()
            val normalized = if(skewSize == 0) 0 else (((maxSkew.toDouble() + 1) / skewSize ) * 10).toInt()
            "$i".padEnd(3, ' ').padEnd(normalized, 'x')
        }, 100)
        logger.debug("Skew detected\n$statStrings")
    }
}
