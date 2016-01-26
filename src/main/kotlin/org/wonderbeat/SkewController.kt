package org.wonderbeat

class SkewController(val maxSkew: Long, val buckets: Array<Long>) {

    fun update(bucket: Int, value: Long) {
        buckets[bucket] = value
    }

    fun isSkewed(): Boolean {
        val minMax = buckets.fold(Pair(buckets.first(), buckets.first()), { acc, item ->
            Pair(if (acc.first < item) acc.first else item, if(acc.second > item) acc.second else item )
        })
        return minMax.second - minMax.first > maxSkew
    }
}
