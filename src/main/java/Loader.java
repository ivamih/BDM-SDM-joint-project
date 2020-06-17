import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.*;


public class Loader {
    private Session session;
    Loader(Session session)
    {
        this.session = session;
    }

    public static String executeTransaction(Session session, String file){
        /* Before uploading to Nao4j do:
            CREATE (node:User), (node:Point), (node:Path)
            CREATE CONSTRAINT ON (p:User) ASSERT p.id IS UNIQUE;
            CREATE CONSTRAINT ON (p:Point) ASSERT p.id IS UNIQUE;
            CREATE CONSTRAINT ON (p:Path) ASSERT p.path_id IS UNIQUE;
         */
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                if (file.equalsIgnoreCase("skopje_users.csv") || file.equalsIgnoreCase("belgrade_users.csv") ){
                    result = tx.run( "LOAD CSV FROM 'file:///" + file + "' AS row\n" +
                            "WITH toInteger(row[0]) AS id, row[1] AS name, row[2] AS car_owner\n" +
                            "MERGE (p:User {id: id})\n" +
                            "SET p.name = name, p.car_owner = car_owner\n" +
                            "RETURN count(p) as users");
                }
                else if (file.equalsIgnoreCase("skopje_nodes.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS id, row[1] AS lat, row[2] AS lon\n" +
                            " MERGE (p:Point {id: id})\n" +
                            " SET p.lat = lat, p.long = lon, p.city ='Skopje'\n" +
                            " RETURN count(p) as skopje_nodes");
                }
                else if (file.equalsIgnoreCase("skopje_ways.csv")) {
                    result = tx.run("LOAD CSV FROM 'file:///" + file + "' AS row\n" +
                            " WITH toInteger(row[1]) AS point_id_1, toInteger(row[2]) AS point_id_2, row[0] AS way_id, toInteger(row[3]) AS weight\n" +
                            " MATCH (p1:Point {id: point_id_1})\n" +
                            " MATCH (p2:Point {id: point_id_2})\n" +
                            " MERGE (p1)-[rel:way]->(p2)\n" +
                            " SET rel.name = way_id, rel.weight = weight\n" +
                            " RETURN count(rel) as skopje_ways");
                }
                else if (file.equalsIgnoreCase("belgrade_nodes.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS id, row[1] AS lat, row[2] AS lon\n" +
                            " MERGE (p:Point {id: id})\n" +
                            " SET p.lat = lat, p.long = lon, p.city ='Belgrade'\n" +
                            " RETURN count(p) as belgrade_nodes");
                }
                else if (file.equalsIgnoreCase("belgrade_ways.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[1]) AS point_id_1, toInteger(row[2]) AS point_id_2, row[0] AS way_id, toInteger(row[3]) AS weight\n" +
                            " MATCH (p1:Point {id: point_id_1})\n" +
                            " MATCH (p2:Point {id: point_id_2})\n" +
                            " MERGE (p1)-[rel:way]->(p2)\n" +
                            " SET rel.name = way_id, rel.weight = weight\n" +
                            " RETURN count(rel) as belgrade_ways");
                }
                else if (file.equalsIgnoreCase("skopje_paths_nodes.csv")){
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS path_id, row[1] AS repeatable_route, row[2] AS time_of_day\n" +
                            " MERGE (p:Path {id: path_id})\n" +
                            " SET p.path_id = path_id, p.repeatable_route = repeatable_route, p.time_of_day = time_of_day, p.city ='Skopje'\n" +
                            " RETURN count(p)");
                }
                else if (file.equalsIgnoreCase("belgrade_paths_nodes.csv")){
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS path_id, row[1] AS repeatable_route, row[2] AS time_of_day\n" +
                            " MERGE (p:Path {id: path_id})\n" +
                            " SET p.path_id = path_id, p.repeatable_route = repeatable_route, p.time_of_day = time_of_day, p.city ='Belgrade'\n" +
                            " RETURN count(p)");
                }
                else if (file.equalsIgnoreCase("skopje_takes.csv") || file.equalsIgnoreCase("belgrade_takes.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS user_id, toInteger(row[1]) AS path_id\n" +
                            " MATCH (u:User {id: user_id})\n" +
                            " MATCH (p:Path {id: path_id})\n" +
                            " MERGE (u)-[rel:takes]->(p)\n" +
                            " RETURN count(rel)");
                }
                //                else if (file.equalsIgnoreCase("skopje_user_path_edge.csv"))
                //                {
                //                    result = tx.run( "LOAD CSV WITH HEADERS FROM 'file:///" + file
                // +"' AS row\n" +
                //                            " WITH toInteger(row.user_id) AS user_id,
                // toInteger(row.path_id) AS path_id\n" +
                //                            " MATCH (u:User {id: user_id})\n" +
                //                            " MATCH (p:Path {id: path_id})\n" +
                //                            " MERGE (u)-[rel:takesPath]->(p)\n" +
                //                            " RETURN count(rel)");
                //                }
                return result.single().toString();
              }
            });
    return res;
  }

//   this method adds new way edges in order to connect disconnected subgraphs
  public String loadAdditionalWays(Session session, String city) {
    EdgeCreator ec = new EdgeCreator();
    List<Tuple2<Long, Long>> pairs = new ArrayList<>(ec.generatePairs(session, city));

    String res = "";

    for (Tuple2<Long, Long> pair : pairs) {
      long num1 = pair._1;
      long num2 = pair._2;

      String id = String.valueOf(num1) + String.valueOf(num2);

      res = session.writeTransaction(new TransactionWork<String>() {
        Result result;

        @Override
        public String execute(Transaction tx) {
          result = tx.run("MATCH (p1:Point), (p2:Point)\n"
                  + "WHERE p1.id = " + num1 + " and p2.id = " + num2 + "\n"
                  + "MERGE (p1)-[w:way]->(p2)\n"
                  + "SET w.id = '" + id + "', w.weight = 1000\n"
                  + "RETURN count(w)");

          return result.single().toString();
        }
      });
    }
    return res;
  }


  String loadNearPoints(String filename) {
    return session
        .run(
            "USING PERIODIC COMMIT 50000\n"
                + "LOAD CSV FROM 'file:///"
                + filename
                + "' AS row\n"
                + " WITH toInteger(row[0]) AS point_id_1, toInteger(row[1]) AS point_id_2\n"
                + " MATCH (p1:Point {id: point_id_1})\n"
                + " MATCH (p2:Point {id: point_id_2})\n"
                + " MERGE (p1)-[rel:near]->(p2)\n"
                + " RETURN count(rel)")
        .peek()
        .toString();
  }

  String findRideShareRecommendations(String pathId) {
    Map<String, Object> params = new HashMap<>();
    params.put("pathId", pathId);
    String query =
        "MATCH (requestedPath: Path {id: $pathId)),(requestedPath)-[:start]->(requesterStartPoint), (requestedPath)-[:end]->(requesterEndPoint)\n"
            + "MATCH (offeredPath: Path)-[:start]-()-[:near]->(requesterStartPoint), (offeredPath: Path)-[:end]-()-[:near]->(requesterEndPoint)\n"
            + "WHERE abs(offeredTime - requestedTime) <= 20mins\n"
            + "MATCH (offeringUser: User)-[:has_path]->(offeredPath)\n"
            + "RETURN offeringUser";
    List<Record> list = session.readTransaction((tx) -> tx.run(query, params)).list();
    return list.toString();
  }
}
