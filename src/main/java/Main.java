import org.apache.spark.SparkConf;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.spark.Neo4j;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.List;

public class Main {
  static String HADOOP_COMMON_PATH =
      "C:\\Users\\Iva\\Desktop\\UPC\\BDM\\Project\\joint-project\\src\\main\\resources\\winutils";

  public static String executeTransaction(Session session, String file) {
    // previousy create the node User and add constraint for unique Id in Neo4j
    String res =
        session.writeTransaction(
            new TransactionWork<String>() {
              @Override
              public String execute(Transaction tx) {
                Result result =
                    tx.run(
                        "LOAD CSV WITH HEADERS FROM 'file:///"
                            + file
                            + "' AS row\n"
                            + "WITH toInteger(row.ID) AS id, row.Name AS name, row.CarOwner AS car_owner\n"
                            + "MERGE (p:User {id: id})\n"
                            + "SET p.name = name, p.car_owner = car_owner\n"
                            + "RETURN count(p)");
                return result.single().toString();
              }
            });
    return res;
  }

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
    SparkConf conf = new SparkConf();
    setupNeo4JConfiguration(conf);
    SparkSession spark =
        SparkSession.builder().appName("GO2").master("local[*]").config(conf).getOrCreate();
    Neo4j neo = new Neo4j(spark.sparkContext());
    exampleSparkReadingFromNeo4j(spark, neo);
  }

  private static void exampleSparkReadingFromNeo4j(SparkSession spark, Neo4j neo) {
    Reader reader = new Reader(spark, neo);
    Dataset<Row> dataset = reader.readAsDataFrame("MATCH (n:Person) RETURN id(n) as id ");
    dataset.show();

    Graph<Long, String> graph =
        reader.readAsGraph(
            "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as source, id(m) as target, type(r) as value SKIP 10 LIMIT 1000");

    System.out.println(graph.vertices().count());
    System.out.println(graph.edges().count());
    List<Edge<String>> collects = graph.edges().toJavaRDD().collect();
    for (Edge<String> a : collects) {
      System.out.println(a.toString());
    }

    Graph<Object, Object> graph2 =
        PageRank.run(
            graph,
            5,
            0.15,
            ClassTag$.MODULE$.apply(Double.class),
            ClassTag$.MODULE$.apply(Double.class));

    List<Tuple2<Object, Object>> collect = graph2.vertices().toJavaRDD().collect();
    for (Tuple2<Object, Object> a : collect) {
      System.out.println(a._1.toString() + ":" + a._2.toString());
    }
  }

  private static void setupNeo4JConfiguration(SparkConf conf) {
    conf.set("spark.neo4j.bolt.url", "bolt://localhost:7687");
    conf.set("spark.neo4j.bolt.user", "neo4j");
    conf.set("spark.neo4j.bolt.password", "iva");
  }
}
