import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.neo4j.spark.Neo4j;

public class Reader {
  private final SparkSession sparkSession;
  private final Neo4j neo;

  Reader(SparkSession sparkSession, Neo4j neo) {
    this.sparkSession = sparkSession;
    this.neo = neo;
  }

  Dataset<Row> readAsDataFrame(String query) {
    return neo.cypher(query, Utils.emptyImmutableScalaMap()).loadDataFrame();
  }

  // Query should be of form: "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as source, id(m)
  // as target, type(r) as value SKIP 10 LIMIT 1000";
  Graph<Long, String> readAsGraph(String query) {
    return neo.rels(query, Utils.emptyImmutableScalaMap())
        .partitions(7)
        .batch(200)
        .loadGraph(
            scala.reflect.ClassTag$.MODULE$.apply(Long.class),
            scala.reflect.ClassTag$.MODULE$.apply(String.class));
  }
}
