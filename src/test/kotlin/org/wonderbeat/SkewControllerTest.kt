package org.wonderbeat

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class SkewControllerTest {
    @Test fun testSkewController() {
        val controller = SkewController(2,2)
        controller.update(1)
        assertFalse { controller.isSkewed() }
        controller.update(1)
        assertFalse { controller.isSkewed() }
        controller.update(1)
        assertTrue { controller.isSkewed() }
    }

}

