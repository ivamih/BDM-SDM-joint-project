import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

public class CSVGenerator {

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

    public static void main(String[] args) {
        generatePathToPointEdge("src/main/resources/skopje_nodes.csv/part-00000",
                "src/main/resources/skopje_paths.csv/part-00000",
                "src/main/resources/skopje_path_to_points.csv");
    }
}
