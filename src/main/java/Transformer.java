import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

public class Transformer {
    public static JavaRDD transformNodes (SparkSession sparkSession, String file)
    {
        Dataset<Row> dataset = sparkSession.read().json(file);
        dataset.printSchema();
        Dataset<Row> d = dataset.select(functions.explode(dataset.col("e")).as("e"),dataset.col("la"), dataset.col("lo"));
        JavaRDD rdd = d.toJavaRDD();
        JavaRDD nodes = rdd.map(t ->
        {
            String a = t.toString().split(",")[2];
            String b = t.toString().split(",")[3];
            String[] id1 = a.split("\\.");
            String[] id2 = b.substring(0,b.length()-1).split("\\.");
            String id = id1[0] + id1[1] + id2[0] +id2[1];
            return id+"," + a+","+b.substring(0,b.length()-1);
        }).distinct();

        if (file.contains("skopje"))
        {
            nodes.saveAsTextFile("src/main/resources/skopje_nodes.csv");
        }
        else if (file.contains("belgrade")) {
            nodes.saveAsTextFile("src/main/resources/belgrade_nodes.csv");
        }
        return rdd;
    }
    public static void transformEdges (JavaRDD rdd, String file)
    {
        JavaPairRDD<Integer, String> edges = rdd.mapToPair(t ->
        {
            String []list = t.toString().split(",");
            String i = list[0]; // edge number
            String w = list[1]; // edge weight
            String la = list[2];
            String lo = list[3];
            String[] id1 = la.split("\\.");
            String[] id2 = lo.substring(0,lo.length()-1).split("\\.");
            String id = id1[0] + id1[1] + id2[0] +id2[1];
            String res = id+"," + la+","+lo.substring(0,lo.length()-1)+"," +i.substring(2,i.length())+","+w.substring(0,w.length()-1);
            // KEY = edge number, VALUE = id_node + la + lo + edge_number + edge_weight
            return new Tuple2<Integer, String>(Integer.valueOf(i.substring(2,i.length())),res);
        });

        // skip edges from n1 to n1
        JavaPairRDD<Integer, Tuple2<String, String>> joined = edges.join(edges).filter(t-> !t._2._1.equals(t._2._2));

//        joined.foreach(t-> System.out.println(t));
        JavaRDD<String> joined_csv = joined.map(t->
        {
            String t1 = t._2._1.split(",")[0];
            String t2 = t._2._2.split(",")[0];
            String s = t._1.toString()+t1.substring(t1.length()-2,t1.length())+t2.substring(t2.length()-2,t2.length())+","+t1+","+t2+","+t._2._1.split(",")[4];
            return s;
        });
        if (file.contains("skopje"))
        {
            joined_csv.saveAsTextFile("src/main/resources/skopje_ways.csv");
        }
        else if (file.contains("belgrade")) {
            joined_csv.saveAsTextFile("src/main/resources/belgrade_ways.csv");
        }


    }

}
