import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import items.UniqueItem;
import servlet.testServlet;

import java.io.File;
import java.sql.Array;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

public class Server {
    private static void execTomcat() {
        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
        tomcat.setPort(8080);
        tomcat.getConnector();
        tomcat.addWebapp("/spark", new File("src/main/resources/").getAbsolutePath());
        tomcat.addServlet("/spark", "testServlet", new testServlet()).addMapping("/testServlet");
    }

    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Project 1 Anthony");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> rows = sparkContext.textFile("src/main/resources/TestData.csv").cache();
        // Function<String, Boolean> filter = k -> (k.matches("Unique+"));
        JavaRDD<String> itemRowsOnly = rows.filter(r -> {
            return r.matches(".*Unique.*");
        }).cache();

        // System.out.println(itemRowsOnly.collect());
        for (String line : itemRowsOnly.collect()) {
            System.out.println(line + "3");
        }
        sparkContext.close();
        // execTomcat();

        // Runtime.getRuntime().addShutdownHook(new Thread() {
        // @Override
        // public void run() {
        // sparkContext.close();
        // }
        // });
    }
}
