import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.rdd.JdbcRDD;
import scala.Tuple2;

@WebServlet("/hello")
public class HelloServlet extends HttpServlet {
    JavaSparkContext sparkContext;
    List<String> names;

    @Override
    public void init() {
        SparkConf conf = new SparkConf().setAppName("NameCounter").setMaster("local");
        sparkContext = new JavaSparkContext(conf);

        names = new ArrayList<>();
        names.add("Mehrab");
        names.add("Sutter");
        names.add("Sutter");
        names.add("Sutter");
        names.add("Sutter");
        names.add("Sutter");
        names.add("Sutter");
        names.add("Sutter");
        names.add("Jeremy");
        names.add("Jeremy");
        names.add("Jeremy");
        names.add("Daniel");
        names.add("Daniel");
        names.add("Daniel");
        names.add("Daniel");
        names.add("Daniel");
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.getWriter().println("Hello World");
//        String name = req.getParameter("name");
//        String lang = req.getParameter("lang");
//        if (name != null && lang != null) {
//            names.add(name);
//            if (lang.equals("en")) {
//                resp.getWriter().println(new HelloGreeter().greet(name));
//            } else if (lang.equals("es")) {
//                Greetable holaGreeter = name1 -> "Hola, " + name1;
//                resp.getWriter().println(holaGreeter.greet(name));
//            } else if (lang.equals("de")) {
//                Greetable gutentagGreetable = (n) -> {
//                    return "Guten tag, " + n;
//                };
//                resp.getWriter().println(gutentagGreetable.greet(name));
//                Runnable runner = () -> {
//                    System.out.println("Running thread");
//                };
//                new Thread(runner).start();
//            }
//        }
//
//        for (String n : names) {
//            resp.getWriter().println(n);
//        }
//        names.stream().map((n) -> {
//            return n + "!";
//        }).forEach(System.out::println);
//        names.forEach(System.out::println);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        JavaRDD<String> namesRDD = sparkContext.parallelize(names);
        JavaPairRDD<String, Integer> namesMapper = namesRDD.mapToPair((f) -> new Tuple2<>(f, 1));
        System.out.println(namesMapper.collect());
        JavaPairRDD<String, Integer> countNames = namesMapper.reduceByKey((x, y) -> (x + (int) y));
        resp.getWriter().println(countNames.collect());
    }
}
