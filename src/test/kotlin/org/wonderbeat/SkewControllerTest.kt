package org.wonderbeat

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class SkewControllerTest {
    @Test fun testSkewController() {
        val controller = SkewController(2, listOf(1,2))
        controller.tryAdvance(1)
        assertFalse { controller.isSkewed() }
        controller.tryAdvance(1)
        assertFalse { controller.isSkewed() }
        controller.tryAdvance(1)
        assertTrue { controller.isSkewed() }
        controller.tryAdvance(2)
        assertFalse { controller.isSkewed() }
        controller.tryAdvance(2)
        assertFalse { controller.isSkewed() }
        controller.tryAdvance(2)
        assertFalse { controller.isSkewed() }
        controller.tryAdvance(2)
        assertTrue { controller.isSkewed() }
    }

    @Test fun testAdvance() {
        val controller = SkewController(2, listOf(0,1,2))
        assert(controller.tryAdvance(1))
        controller.tryAdvance(1)
        controller.tryAdvance(1)
        assertTrue { controller.isSkewed() }
        assertFalse(controller.tryAdvance(1))
        assertTrue(controller.tryAdvance(0))
    }

}

