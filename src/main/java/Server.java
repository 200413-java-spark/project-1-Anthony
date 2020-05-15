import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import io.SQLIo;
import io.parseCSVFile;
import io.netty.util.internal.SystemPropertyUtil;
import items.UniqueItem;
import scala.Tuple2;
import servlet.testServlet;
import spark.sparkOperations;

import java.io.File;
import java.sql.Array;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
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
            // args 1 is sc data args 2 is hc data
            JavaRDD<UniqueItem> itemList = new parseCSVFile().createItemRDD(sparkContext, args[1]);
            JavaRDD<UniqueItem> itemListHC = new parseCSVFile().createItemRDD(sparkContext, args[2]);
            List<UniqueItem> listItems = itemList.collect();
            List<UniqueItem> listItemsHC = itemListHC.collect();
            sparkOperations SparkOperations = new sparkOperations(itemList);
            sparkOperations SparkOperationsHC = new sparkOperations(itemListHC);
            List<Tuple2<String, Double>> list = SparkOperations.getDataSparked(itemList);
            List<Tuple2<String, Double>> listHC = SparkOperationsHC.getDataSparked(itemListHC);
            // args 3 is sc tablename args 4 is hctablename
            JavaRDD<Tuple2<String, Double>> unionSC = sparkContext.parallelize(list);
            JavaRDD<Tuple2<String, Double>> unionHC = sparkContext.parallelize(listHC);

            JavaPairRDD<String, Double> union = unionSC.join(unionHC);
            System.out.println(union.take(5));
            // SQLIo.insertSQL(listItems, args[3]);
            // SQLIo.insertAverages(list, args[3]);
            // SQLIo.insertSQL(listItemsHC, args[4]);
            // SQLIo.insertAverages(listHC, args[4]);
            sparkContext.close();
            // execTomcat();

        } else {
            System.out.println("not loaded" + args[0]);
            // execTomcat();
        }

    }
}
