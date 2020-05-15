package spark;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import items.UniqueItem;

public class sparkOperations {
  JavaRDD<UniqueItem> items;
  private HashMap<String, String> dataAfterSparked = new HashMap<String, String>();

  public sparkOperations(JavaRDD<UniqueItem> items) {
    this.items = items;
    this.dataAfterSparked.put("average", avgValue());
  }

  public List<Tuple2<String, Double>> getDataSparked(JavaRDD<UniqueItem> items) {
    return items.mapToPair((x) -> new Tuple2<>(x.name, (double) x.value)).mapValues(f -> new Tuple2<>(f, 1))
        .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)).mapValues(f -> Precision.round(f._1 / f._2, 1))
        .collect();
  }

  public String avgValue() {
    return items.mapToPair((x) -> new Tuple2<>(x.name, (double) x.value)).mapValues(f -> new Tuple2<>(f, 1))
        .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)).mapValues(f -> Precision.round(f._1 / f._2, 1))
        .collect().toString();
  }

  // public String topGainers() {
  // return items.mapToPair(f -> new Tuple2<>(f.name, (double) f.value)).max((x,y)
  // -> x._1 + y._1).collect().toSting();
  // }
}