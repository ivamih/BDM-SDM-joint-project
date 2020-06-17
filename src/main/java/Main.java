import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

public class Main {

  static String HADOOP_COMMON_PATH =
      "C:\\Users\\Iva\\Desktop\\UPC\\BDM\\Project\\joint-project\\src\\main\\resources\\winutils";

  public static void main(String[] args) {
    Driver driver;
    Transformer transformer = new Transformer();
    EdgeCreator ec = new EdgeCreator();

    System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
    LogManager.getRootLogger().setLevel(Level.ERROR);
    LogManager.shutdown();

    SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
    JavaSparkContext ctx = new JavaSparkContext(conf);

    driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
    Session session = driver.session();
    Loader loader = new Loader(session);

        SparkSession spark_session = SparkSession.builder().master("local").appName("GO2").getOrCreate();
        Processor processor = new Processor(spark_session);

//        TRANSFORM USER ID
//        transformer.transformUsers(spark_session, "src/main/resources/Susers.csv");
//        transformer.transformUsers(spark_session, "src/main/resources/Busers.csv");

////       CONVERT JSON TO CSV nodes ( Skopje )
//        JavaRDD rdd = transformer.transformNodes(spark_session, "src/main/resources/skopje_graph.json");
//
////      TRANSFORM EDGES BETWEEN SKOPJE NODES
//        transformer.transformEdges(rdd,"src/main/resources/skopje_graph.json");
//
////       CONVERT JSON TO CSV nodes ( Belgrade )
//        JavaRDD rddB = transformer.transformNodes(spark_session, "src/main/resources/belgrade_graph.json");
//
////      TRANSFORM EDGES BETWEEN BELGRADE NODES
//        transformer.transformEdges(rddB,"src/main/resources/belgrade_graph.json");

    ////        CREATE SKOPJE PATH NODES
    //        transformer.transformPaths(ctx, "src/main/resources/skopje_paths_no_time.csv");
    //
    ////        CREATE BELGRADE PATH NODES
    //        transformer.transformPaths(ctx, "src/main/resources/belgrade_paths_no_time.csv");

    //        ec.generatePathToPointEdge(ctx, "src/main/resources/skopje_nodes.csv/part-00000",
    //                "src/main/resources/skopje_paths_nodes.csv/part-00000",
    //                "src/main/resources/skopje_path_to_points.csv");
    //
    //        ec.generatePathToPointEdge(ctx, "src/main/resources/belgrade_nodes.csv/part-00000",
    //                "src/main/resources/belgrade_paths_nodes.csv/part-00000",
    //                "src/main/resources/belgrade_path_to_points.csv");

    ////            Infer BELGRADE near Nodes
    //        processor.inferNearPoints("src/main/resources/belgrade_nodes.csv",
    // "src/main/resources/belgrade_near_edges.csv",0.5);
    ////            Infer SKOPJE near Nodes
    //        processor.inferNearPoints("src/main/resources/skopje_nodes.csv",
    // "src/main/resources/skopje_near_edges.csv",0.5);

    //        UPLOAD to NEO4J
    //        System.out.println( loader.executeTransaction(session, "skopje_users.csv"));
    //        System.out.println( loader.executeTransaction(session, "belgrade_users.csv"));
    //        System.out.println( loader.executeTransaction(session, "skopje_nodes.csv") );
    //        System.out.println( loader.executeTransaction(session, "skopje_ways.csv") );
    //        System.out.println( loader.executeTransaction(session, "belgrade_nodes.csv") );
    //        System.out.println( loader.executeTransaction(session, "belgrade_ways.csv") );
    //    System.out.println(loader.executeTransaction(session, "skopje_paths_nodes.csv"));
    //    System.out.println(loader.executeTransaction(session, "belgrade_paths_nodes.csv"));
    //    System.out.println(loader.executeTransaction(session, "belgrade_path_to_points.csv"));
    //    System.out.println(loader.executeTransaction(session, "skopje_path_to_points.csv"));



//        System.out.println(loader.loadAdditionalWays(session, "Belgrade"));
//        System.out.println(loader.loadAdditionalWays(session, "Skopje"));

//                System.out.println(loader.loadNearPoints("belgrade_near_edges.csv"));
//                System.out.println(loader.loadNearPoints("skopje_near_edges.csv"));

        //    Valid recommendation
        System.out.println(loader.naiveFindRideShareRecommendations(22265, 22271));
        //    No recommendation search
        System.out.println(loader.naiveFindRideShareRecommendations(1, 1));

        // Naive recommendation, still not working, until associations are defined and we run and
        // identify time functions
//            System.out.println(loader.findRideShareRecommendations("11180"));

        spark_session.stop();
        session.close();
        driver.close();
    }
}
