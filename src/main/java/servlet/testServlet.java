package servlet;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class testServlet extends HttpServlet {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void init() {

  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.getWriter().println("Hello World");

  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    // JavaRDD<String> namesRDD = sparkContext.parallelize(names);
    // JavaPairRDD<String, Integer> namesMapper = namesRDD.mapToPair((f) -> new
    // Tuple2<>(f, 1));
    // System.out.println(namesMapper.collect());
    // JavaPairRDD<String, Integer> countNames = namesMapper.reduceByKey((x, y) ->
    // (x + (int) y));
    // resp.getWriter().println(countNames.collect());
  }
}