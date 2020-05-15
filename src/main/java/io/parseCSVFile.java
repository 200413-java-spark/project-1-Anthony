package io;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import items.UniqueItem;

public class parseCSVFile {
  private String textFile;

  public JavaRDD<UniqueItem> createItemRDD(JavaSparkContext sparkContext, String textFile) {
    this.textFile = textFile;
    JavaRDD<String> rows = sparkContext.textFile(textFile).cache();
    // Function<String, Boolean> filter = k -> (k.matches("Unique+"));
    JavaRDD<String> itemRowsOnly = rows.filter(r -> {
      return r.matches(".*Unique.*");
    }).cache();

    JavaRDD<UniqueItem> arrayRows = itemRowsOnly.map(r -> {
      String[] cols = r.split(";");
      return new UniqueItem(cols[0], cols[1], Integer.parseInt(cols[2]), cols[3], cols[4], cols[5],
          Double.parseDouble(cols[8]));
    }).cache();
    return arrayRows;
  }

  // public static List<UniqueItem> createListFromRDD(JavaRDD<UniqueItem> RDD) {
  // List<UniqueItem> list = new List<UniqueItem>();
  // RDD.foreach(f -> {
  // list.add(f);
  // });
  // return list;
  // }
}