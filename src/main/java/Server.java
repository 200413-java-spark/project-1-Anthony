import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import io.SQLIo;
import io.parseCSVFile;
import io.netty.util.internal.SystemPropertyUtil;
import items.UniqueItem;
import servlet.testServlet;

import java.io.File;
import java.sql.Array;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;

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

    public static void main(String args[]) throws SQLException {
        // System.out.println("args: " + args[0]);
        if (args[0].equals("load")) {
            SparkConf conf = new SparkConf().setMaster("local").setAppName("Project 1 Anthony");
            JavaSparkContext sparkContext = new JavaSparkContext(conf);
            JavaRDD<UniqueItem> itemList = new parseCSVFile().createItemRDD(sparkContext);
            System.out.println("itemlist: " + itemList.count());
            // List<UniqueItem> arrayList = parseCSVFile.createListFromRDD(itemList);
            itemList.foreach(f -> {
                System.out.println(f.getName());
            });

            sparkContext.close();
            // execTomcat();

            // Runtime.getRuntime().addShutdownHook(new Thread() {
            // @Override
            // public void run() {
            // sparkContext.close();
            // }
            // });
        } else {
            System.out.println("not loaded" + args[0]);
            // execTomcat();
        }

    }
}
