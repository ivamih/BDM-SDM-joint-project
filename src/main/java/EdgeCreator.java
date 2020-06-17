import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.neo4j.driver.*;
import scala.Tuple2;

import static java.lang.Long.parseLong;


public class EdgeCreator {
    static String HADOOP_COMMON_PATH = "C:\\Users\\Iva\\Desktop\\UPC\\BDM\\Project\\joint-project\\src\\main\\resources\\winutils";

    public static void generateUserToPathEdge(JavaSparkContext ctx, String paths_file, String users_file, String output_file)
    {
        String line = "";
        ArrayList<String> al = new ArrayList<String>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(paths_file)); // all path nodes

            while ((line = br.readLine()) != null) {
                String[] point = line.split(",");
                //get all ids of the path nodes from certain town
                al.add(point[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Random ran = new Random();

        JavaRDD<String> testRDD = ctx.textFile(users_file);

        // header = user_id,path_id
        JavaRDD<String> test1RDD = testRDD.map(t -> t.split(",")[0] + "," + al.get(ran.nextInt(al.size()))).coalesce(1);
        test1RDD.saveAsTextFile(output_file);
    }

    public static void generatePathToPointEdge(String points_file, String paths_file, String output_file) {
        String line = "";
        ArrayList<String> al = new ArrayList<String>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(points_file)); // points

            while ((line = br.readLine()) != null) {
                String[] point = line.split(",");
                al.add(point[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Random ran = new Random();

        SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> testRDD = ctx.textFile(paths_file);

        // header = path_id,start_point_id,end_point_id
        JavaRDD<String> test1RDD = testRDD.map(t -> t.split(",")[0] + "," + al.get(ran.nextInt(al.size())) + "," + al.get(ran.nextInt(al.size()))).coalesce(1);
        test1RDD.saveAsTextFile(output_file);
    }

    public static String weaklyConnectedComponents(Session session, String city) {
        String res = session.writeTransaction(new TransactionWork<String>() {
            Result result;

            @Override
            public String execute(Transaction tx) {
                    result = tx.run("CALL algo.unionFind.stream('Point', 'way')\n"
                            + "YIELD nodeId, setId\n"
                            + "WHERE algo.asNode(nodeId).city = "
                            + "'" + city + "'\n"
                            + "WITH count(algo.asNode(nodeId).id) AS Point_count, collect(algo.asNode(nodeId).id) AS Point_collection, setId\n"
                            + "RETURN Point_collection[0] as firstPoint");

                return result.list(res -> res.values().get(0)).toString();
            }
        });
        return res;
    }

    public List<Tuple2<Long, Long>> generatePairs(Session session, String city) {
        List<Tuple2<Long, Long>> pairs = new ArrayList<>();
        String weaklyConnectedPoints = weaklyConnectedComponents(session, city).replaceAll("[\\[\\]]", "");
        List<String> points = new ArrayList<String>(Arrays.asList(weaklyConnectedPoints.split(",")));

        for (int i = 0; i < points.size()-1; i++) {
            Tuple2<Long, Long> tuple = new Tuple2<>((Long.parseLong(points.get(i).trim())), Long.parseLong(points.get(i+1).trim()));
            pairs.add(tuple);
        }

        return pairs;
    }


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        SparkConf conf = new SparkConf().setAppName("GO2").setMaster("local[*]");
        Driver driver = GraphDatabase.driver("bolt://localhost:11002", AuthTokens.basic("neo4j", "iva"));

        Session session = driver.session();
        Loader loader = new Loader(session);
        JavaSparkContext ctx = new JavaSparkContext(conf);

//          CREATE SKOPJE PATH TO POINT EDGES
//        generatePathToPointEdge("src/main/resources/skopje_nodes.csv/part-00000",
//                "src/main/resources/skopje_paths_nodes.csv/part-00000",
//                "src/main/resources/skopje_path_to_points.csv");

////        CREATE SKOPJE TAKES EDGES (USER-> PATH)
////        user_id, path_id
//        generateUserToPathEdge(ctx, "src/main/resources/skopje_paths_nodes.csv/part-00000",
//                "src/main/resources/skopje_users.csv/part-00000",
//                "src/main/resources/skopje_takes.csv");
//
////        CREATE BELGRADE TAKES EDGES (USER-> PATH)
////        user_id, path_id
//        generateUserToPathEdge(ctx, "src/main/resources/belgrade_paths_nodes.csv/part-00000",
//                "src/main/resources/belgrade_users.csv/part-00000",
//                "src/main/resources/belgrade_takes.csv");

        //    LOAD TAKES EDGES INTO NEO4J
        System.out.println( loader.executeTransaction(session, "skopje_takes.csv"));
        System.out.println( loader.executeTransaction(session, "belgrade_takes.csv"));

    }
}
