import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.neo4j.driver.internal.shaded.reactor.util.function.Tuple4;
import org.neo4j.spark.*;
import org.neo4j.spark.dataframe.Neo4jDataFrame;
import org.neo4j.driver.*;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import scala.Tuple2;

import static org.neo4j.driver.Values.ofFloat;
import static org.neo4j.driver.Values.parameters;

public class Main {
    static String HADOOP_COMMON_PATH = "C:\\Users\\Iva\\Desktop\\UPC\\BDM\\Project\\joint-project\\src\\main\\resources\\winutils";

    public static String executeTransaction(Session session,String file){
        // previousy create the node User and add constraint for unique Id in Neo4j
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                if (file.equalsIgnoreCase("users.csv")){
                    result = tx.run( "LOAD CSV WITH HEADERS FROM 'file:///" + file + "' AS row\n" +
                            "WITH toInteger(row.ID) AS id, row.Name AS name, row.CarOwner AS car_owner\n" +
                            "MERGE (p:User {id: id})\n" +
                            "SET p.name = name, p.car_owner = car_owner\n" +
                            "RETURN count(p)");
                }
                else if (file.equalsIgnoreCase("skopje_nodes.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS id, row[1] AS lat, row[2] AS lon\n" +
                            " MERGE (p:Point {id: id})\n" +
                            " SET p.lat = lat, p.long = lon, p.city ='Skopje'\n" +
                            " RETURN count(p)");
                }

                return result.single().toString();
            }
        } );
        return res;
    }
    public static void main(String[] args) {
        Driver driver;

        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        LogManager.getRootLogger().setLevel(Level.ERROR);
        LogManager.shutdown();

//        SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
//        JavaSparkContext ctx = new JavaSparkContext(conf);

        driver = GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic("neo4j", "iva"));
        Session session = driver.session();

        SparkSession spark_session = SparkSession.builder().master("local").appName("GO2").getOrCreate();

//        UPLOAD USERS TO NEO4J
        System.out.println( executeTransaction(session, "users.csv") );

//       CONVERT JSON TO CSV nodes ( Skopje )
        Dataset<Row> dataset = spark_session.read().json("src/main/resources/skopje_graph.json");

        dataset.printSchema();
        Dataset<Row> d = dataset.select(functions.explode(dataset.col("e")).as("e"),dataset.col("la"), dataset.col("lo"));
//        d.foreach(item -> {
//            String[] s = item.get(0).toString().split(",");
//            String s1 = s[0];
//            String s2 = s[1];
//            System.out.println(s1.substring(1,s1.length()) + " " + s2.substring(0,s2.length()-1) + " " + item.get(1) + " " + item.get(2));
//        });

        JavaRDD rdd = d.toJavaRDD();
//        d.foreach(item -> {
//            System.out.println(item);
//            String[] s = item.get(0).toString().split(",");
//            String s1 = s[0];
//            String s2 = s[1];
//            System.out.println(s1.substring(1,s1.length()) + " " + s2.substring(0,s2.length()-1) + " " + item.get(1) + " " + item.get(2));
//        });
        JavaRDD nodes = rdd.map(t ->
        {
            String a = t.toString().split(",")[2];
            String b = t.toString().split(",")[3];
            System.out.println(a);
            System.out.println(b);
            String[] id1 = a.split("\\.");
            System.out.println(id1.length);
            String[] id2 = b.substring(0,b.length()-1).split("\\.");
            String id = id1[0] + id1[1] + id2[0] +id2[1];
            return id+"," + a+","+b.substring(0,b.length()-1);
        }).distinct();

        nodes.saveAsTextFile("src/main/resources/skopje_nodes.csv");
        System.out.println( executeTransaction(session, "skopje_nodes.csv") );





//        CsvOutPutFormatPreprocessor<Row> csvOutPutFormatPreprocessor = new CsvOutPutFormatPreprocessor<Row>();
//        Column[] flattened_column = csvOutPutFormatPreprocessor.flattenNestedStructure(d);
//        d.select(flattened_column).write().mode(SaveMode.Overwrite).option("header", "true").format("csv").save("src/main/resources/belgrade");



    }
}
