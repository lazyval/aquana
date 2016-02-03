package org.wonderbeat

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class SkewControllerTest {
    @Test fun testSkewController() {
        val controller = SkewController(2,2)
        controller.tryAdvance(1)
        assertFalse { controller.isSkewed() }
        controller.tryAdvance(1)
        assertFalse { controller.isSkewed() }
        controller.tryAdvance(1)
        assertTrue { controller.isSkewed() }
    }

    @Test fun testAdvance() {
        val controller = SkewController(2,2)
        assert(controller.tryAdvance(1))
        controller.tryAdvance(1)
        controller.tryAdvance(1)
        assertTrue { controller.isSkewed() }
        assertFalse(controller.tryAdvance(1))
        assertTrue(controller.tryAdvance(0))
    }

}

