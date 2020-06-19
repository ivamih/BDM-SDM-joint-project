import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

public class Transformer {

    public static void transformUsers (SparkSession ctx, String file)
    {
        /*
        users file: id, name, CarOwner
        Skopje users - id starts with 111
        Belgrade users - id starts with 222
         */
        Dataset<Row> users = ctx.read().text(file);
        JavaRDD users_rdd = users.toJavaRDD();

        if( file.contains("S"))
        {
            users_rdd = users_rdd.map(t->
            {
                String [] list = t.toString().split(",");
                String id = "111"+list[0].substring(1,list[0].length());
                String name = list[1];
                String car = list[2].substring(0, list[2].length()-1);
                return id+","+name+","+car;
            });
            users_rdd.saveAsTextFile("src/main/resources/skopje_users.csv");
        }
        else if (file.contains("B"))
        {
            users_rdd = users_rdd.map(t->
            {
                String [] list = t.toString().split(",");
                String id = "222"+list[0].substring(1,list[0].length());
                String name = list[1];
                String car = list[2].substring(0, list[2].length()-1);
                return id+","+name+","+car;
            });
            users_rdd.saveAsTextFile("src/main/resources/belgrade_users.csv");
        }
    }

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

    public static void transformWayEdges (JavaRDD rdd, String file)
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

    public static void transformPaths(JavaSparkContext ctx, String file) {
        JavaRDD<String> paths = ctx.textFile(file);

        JavaRDD path_nodes = paths.map(t ->
        {
            String path_id = "";

            if (file.contains("skopje")) {
                path_id = "111"+t.split(",")[0];
            } else if(file.contains("belgrade")) {
                path_id = "222"+t.split(",")[0];
            }
            String repeatable_route = t.split(",")[1];
            String hours = t.split(",")[2];
            String minutes = t.split(",")[3];

            if (hours.length() < 2)
                hours = "0" + hours;
            if (minutes.length() < 2)
                minutes = "0" + minutes;

            String time = hours + ":" + minutes;
            return path_id + "," + repeatable_route + "," + time;
        }).distinct().coalesce(1);

        if (file.contains("skopje")) {
            path_nodes.saveAsTextFile("src/main/resources/skopje_paths_nodes.csv");
        } else if (file.contains("belgrade")) {
            path_nodes.saveAsTextFile("src/main/resources/belgrade_paths_nodes.csv");
        }
    }

    public void transformAllData (JavaSparkContext ctx, SparkSession spark_session) {
        EdgeCreator ec = new EdgeCreator();
        Processor processor = new Processor(spark_session);

        // TRANSFORM USER FILE: TRANSFORM USER ID
        transformUsers(spark_session, "src/main/resources/Susers.csv");
        transformUsers(spark_session, "src/main/resources/Busers.csv");

        // CONVERT JSON TO CSV point nodes ( Skopje ) AND TRANSFORM WAY EDGES BETWEEN SKOPJE NODES
        JavaRDD rddS = transformNodes(spark_session, "src/main/resources/skopje_graph.json");
        transformWayEdges(rddS,"src/main/resources/skopje_graph.json");

        // CONVERT JSON TO CSV point nodes ( Belgrade ) AND TRANSFORM WAY EDGES BETWEEN BELGRADE NODES
        JavaRDD rddB = transformNodes(spark_session, "src/main/resources/belgrade_graph.json");
        transformWayEdges(rddB,"src/main/resources/belgrade_graph.json");

        // CREATE SKOPJE AND BELGRADE PATH NODES
        transformPaths(ctx, "src/main/resources/skopje_paths_no_time.csv");
        transformPaths(ctx, "src/main/resources/belgrade_paths_no_time.csv");

        // CREATE SKOPJE AND BELGRADE TAKES EDGES (USER-> PATH)
        ec.generateUserToPathEdge(ctx, "src/main/resources/skopje_paths_nodes.csv/part-00000",
                "src/main/resources/skopje_users.csv/part-00000",
                "src/main/resources/skopje_takes.csv");
        ec.generateUserToPathEdge(ctx, "src/main/resources/belgrade_paths_nodes.csv/part-00000",
                "src/main/resources/belgrade_users.csv/part-00000",
                "src/main/resources/belgrade_takes.csv");

        // CREATE SKOPJE AND BELGRADE PATH TO POINT EDGE
        ec.generatePathToPointEdge(ctx, "src/main/resources/skopje_nodes.csv/part-00000",
                "src/main/resources/skopje_paths_nodes.csv/part-00000",
                "src/main/resources/skopje_path_to_points.csv");
        ec.generatePathToPointEdge(ctx, "src/main/resources/belgrade_nodes.csv/part-00000",
                "src/main/resources/belgrade_paths_nodes.csv/part-00000",
                "src/main/resources/belgrade_path_to_points.csv");

        // INFER BELGRADE NEAR EDGES
        processor.inferNearPoints("src/main/resources/skopje_nodes.csv", "src/main/resources/skopje_near_edges.csv",0.5);
        processor.inferNearPoints("src/main/resources/belgrade_nodes.csv", "src/main/resources/belgrade_near_edges.csv",0.5);
    }
}
