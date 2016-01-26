package org.wonderbeat;


import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;

public class ToScalaMap {
    public static <A, B> Map<A, B> toScalaMap(java.util.Map<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
                Predef.<Tuple2<A, B>>conforms()
        );
    }
}
