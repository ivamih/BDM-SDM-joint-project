import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import static org.neo4j.driver.Values.parameters;

public class Main {
    static String HADOOP_COMMON_PATH = "C:\\Users\\Iva\\Desktop\\UPC\\BDM\\Project\\joint-project\\src\\main\\resources\\winutils";

    public static String executeTransaction(Session session,String file){
        // previousy create the node User and add constraint for unique Id in Neo4j
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            @Override
            public String execute( Transaction tx )
            {
                Result result = tx.run( "LOAD CSV WITH HEADERS FROM 'file:///" + file + "' AS row\n" +
                        "WITH toInteger(row.ID) AS id, row.Name AS name, row.CarOwner AS car_owner\n" +
                        "MERGE (p:User {id: id})\n" +
                        "SET p.name = name, p.car_owner = car_owner\n" +
                        "RETURN count(p)");
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

        SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<String> users = ctx.textFile("src/main/resources/users.txt");
        driver = GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic("neo4j", "iva"));
        Session session = driver.session();

        System.out.println( executeTransaction(session, "users.csv") );

//        String out = "";
//        out += "The file has "+users.count()+" lines\n";
//        System.out.println(out);
    }
}
