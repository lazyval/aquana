package org.wonderbeat

import org.junit.Test


class PrintTest {

    @Test fun testLongString() {
        var strings = emptyList<String>()
        repeat(5, {
            strings += 90.downTo(1).toList().toString()
        })
        assert(printVertically(strings).split("\n").size == 351)
    }

    @Test fun widePrintTest() {
        var strings = emptyList<String>()
        repeat(90, {
            strings += 3.downTo(1).toList().toString()
        })
        assert(printVertically(strings).split("\n").size == 18)
    }
}
