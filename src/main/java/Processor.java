import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Processor {
    private SparkSession sparkSession;

    Processor(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    void inferNearPoints(String inputPath, String outputPath, double distanceInKilometers) {
        Dataset<Row> points = sparkSession.read().csv(inputPath).cache();
        Dataset<Row> joinedPoints =
                points
                        .as("t")
                        .crossJoin(points.as("o")).
                        select(
                                col("t._c0").as("node_1"),
                                col("o._c0").as("node_2"),
                                col("t._c1").cast("double").as("lat_1"),
                                col("o._c1").cast("double").as("lat_2"),
                                col("t._c2").cast("double").as("lon_1"),
                                col("o._c2").cast("double").as("lon_2"));
        FilterFunction<Row> distanceFilter =
                (FilterFunction<Row>)
                        row -> {
            double lat1 = row.getDouble(row.fieldIndex("lat_1"));
            double lat2 = row.getDouble(row.fieldIndex("lat_2"));
            double lon1 = row.getDouble(row.fieldIndex("lon_1"));
            double lon2 = row.getDouble(row.fieldIndex("lon_2"));
            if ((lat1 == lat2) && (lon1 == lon2)) {
                return true;
            } else {
                double theta = lon1 - lon2;
                double dist =
                        Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                                + Math.cos(Math.toRadians(lat1))
                                * Math.cos(Math.toRadians(lat2))
                                * Math.cos(Math.toRadians(theta));
                dist = Math.acos(dist);
                dist = Math.toDegrees(dist);
                dist = dist * 60 * 1.1515;
                dist = dist * 1.609344;
                return dist <= distanceInKilometers;
            }
        };
        Dataset<Row> nearPoints = joinedPoints.filter(distanceFilter);
        Dataset<Row> nearNodes = nearPoints.select(nearPoints.col("node_1"), nearPoints.col("node_2"));
        nearNodes.write().csv(outputPath);
    }
}
