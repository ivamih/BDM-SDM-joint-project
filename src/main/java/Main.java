import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.spark.Neo4j;
import scala.Predef;
import scala.collection.JavaConverters;

import java.util.HashMap;

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

    SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
    setupNeo4JConfiguration(conf);
    JavaSparkContext ctx = new JavaSparkContext(conf);
    Neo4j neo = new Neo4j(ctx.sc());
    scala.collection.immutable.Map<String, Object> stringObjectMap =
            JavaConverters.mapAsScalaMapConverter(new HashMap<String, Object>()).asScala().toMap(Predef.$conforms());
    Dataset<Row> rowDataset = neo.cypher("MATCH (n:Person) RETURN id(n) as id ", stringObjectMap).loadDataFrame();
    rowDataset.show();
//    JavaRDD<String> users = ctx.textFile("src/main/resources/users.txt");
//    driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "iva"));
//    Session session = driver.session();

    //    System.out.println(executeTransaction(session, "users.csv"));

    //        String out = "";
    //        out += "The file has "+users.count()+" lines\n";
    //        System.out.println(out);
  }

  private static void setupNeo4JConfiguration(SparkConf conf) {
    conf.set("spark.neo4j.bolt.url", "bolt://localhost:7687");
    conf.set("spark.neo4j.bolt.user", "neo4j");
    conf.set("spark.neo4j.bolt.password", "iva");
  }
}
