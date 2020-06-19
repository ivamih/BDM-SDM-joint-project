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
    static String HADOOP_COMMON_PATH = "C:\\Users\\Tamara Bojanic\\Desktop\\UPC\\BDM-SDM-joint-project\\src\\main\\resources\\winutils";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        LogManager.getRootLogger().setLevel(Level.ERROR);
        LogManager.shutdown();

        SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        SparkSession spark_session = SparkSession.builder().master("local").appName("GO2").getOrCreate();

        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));

        Session session = driver.session();
        Transformer transformer = new Transformer();
        Loader loader = new Loader(session);

        if (args[0].equals("-transform")) {
            transformer.transformAllData(ctx, spark_session);
        }
        else if (args[0].equals("-load")) {
            loader.loadAllData(session);
        }
        else if (args[0].equals("-naiveRecommender")) {
            System.out.println(loader.naiveFindRideShareRecommendations(22240, 22237));
            System.out.println(loader.naiveFindRideShareRecommendations(1, 1));
        }
        else if (args[0].equals("-betweenness")) {
            System.out.println(loader.betweenness(session));
        }

        spark_session.stop();
        session.close();
        driver.close();
    }
}
