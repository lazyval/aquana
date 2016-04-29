package org.wonderbeat

import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream
import java.util.stream.StreamSupport

fun <T> invokeConcurrently(vararg  functions: () -> T): Stream<T> = StreamSupport.stream(functions.toCollection(ArrayList()).spliterator(), true).map { it.invoke() }
fun <T> Stream<T>.collectToList(): List<T> = this.collect(Collectors.toList<T>()).toList()
