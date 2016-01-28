package org.wonderbeat

fun printVertically(data: List<String>, stringLength: Int = 80): String {
    val maxLength = data.foldRight(0, { str , acc -> if(str.length > acc) str.length else acc })
    val padded = data.map { it.padEnd(maxLength, ' ') }
    var slice = if(data.size > stringLength) {
        var temp = emptyList<Int>()
        repeat(data.size / stringLength, { temp += stringLength })
        temp += data.size - 1 - temp.size * stringLength
        temp
    } else {
        listOf(data.size)
    }
    return slice.mapIndexed { i, s ->  (0).rangeTo(maxLength - 1).map {
        num -> padded.subList(i * stringLength, i * stringLength + s).map { it[num] }.joinToString("") }
            .joinToString("\n")}.joinToString("\n")
}
