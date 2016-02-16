package org.wonderbeat

import org.junit.Test


class SkewControllerTest {

    @Test fun testAdvance() {
        val controller = SkewController(2, listOf(0,1,2))
        assert(controller.tryAdvance(1))
        controller.tryAdvance(1)
        controller.tryAdvance(1)
        assert(controller.isSkewed() == true)
        assert(controller.tryAdvance(1) == false)
        assert(controller.tryAdvance(0) == true)
    }

    @Test fun testSkewController() {
        val controller = SkewController(2, listOf(1,2))
        controller.tryAdvance(1)
        assert(controller.isSkewed() == false)
        controller.tryAdvance(1)
        assert(controller.isSkewed() == false)
        controller.tryAdvance(1)
        assert(controller.isSkewed() == true)
        controller.tryAdvance(2)
        assert(controller.isSkewed() == false)
        controller.tryAdvance(2)
        assert(controller.isSkewed() == false)
        controller.tryAdvance(2)
        assert(controller.isSkewed() == false)
        controller.tryAdvance(2)
        controller.tryAdvance(2)
        controller.tryAdvance(2)
        assert(controller.isSkewed() == true)
    }

}

