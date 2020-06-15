import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;


public class Main {

    static String HADOOP_COMMON_PATH = "C:\\Users\\Tamara Bojanic\\Desktop\\UPC\\BDM-SDM-joint-project\\src\\main\\resources\\winutils";

    public static void main(String[] args) {
        Driver driver;
        Loader loader = new Loader();
        Transformer transformer = new Transformer();

        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        LogManager.getRootLogger().setLevel(Level.ERROR);
        LogManager.shutdown();

        SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        driver = GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();

        SparkSession spark_session = SparkSession.builder().master("local").appName("GO2").getOrCreate();

//       CONVERT JSON TO CSV nodes ( Skopje )
        JavaRDD rdd = transformer.transformNodes(spark_session, "src/main/resources/skopje_graph.json");

//      TRANSFORM EDGES BETWEEN SKOPJE NODES
        transformer.transformEdges(rdd,"src/main/resources/skopje_graph.json");

//       CONVERT JSON TO CSV nodes ( Belgrade )
        JavaRDD rddB = transformer.transformNodes(spark_session, "src/main/resources/belgrade_graph.json");

//      TRANSFORM EDGES BETWEEN BELGRADE NODES
        transformer.transformEdges(rddB,"src/main/resources/belgrade_graph.json");

//        UPLOAD to NEO4J
        System.out.println( loader.executeTransaction(session, "users.csv"));
        System.out.println( loader.executeTransaction(session, "skopje_nodes.csv") );
        System.out.println( loader.executeTransaction(session, "skopje_ways.csv") );
        System.out.println( loader.executeTransaction(session, "belgrade_nodes.csv") );
        System.out.println( loader.executeTransaction(session, "belgrade_ways.csv") );

    }
}
