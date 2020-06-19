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

import static java.util.stream.Collectors.joining;


public class Loader {
    private Session session;

    Loader(Session session)
    {
        this.session = session;
    }

    /* Before uploading to Nao4j do:
       CREATE (node:User), (node:Point), (node:Path)
       CREATE CONSTRAINT ON (p:User) ASSERT p.id IS UNIQUE;
       CREATE CONSTRAINT ON (p:Point) ASSERT p.id IS UNIQUE;
       CREATE CONSTRAINT ON (p:Path) ASSERT p.path_id IS UNIQUE;
    */

    public static String loadUsers(Session session, String file){
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                result = tx.run( "LOAD CSV FROM 'file:///" + file + "' AS row\n" +
                        "WITH toInteger(row[0]) AS id, row[1] AS name, row[2] AS car_owner\n" +
                        "MERGE (p:User {id: id})\n" +
                        "SET p.name = name, p.car_owner = car_owner\n" +
                        "RETURN count(p) as users");

                return result.single().toString();
            }
        });
        return res;
    }

    public static String loadPointNodes(Session session, String file){
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                if (file.equalsIgnoreCase("skopje_nodes.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS id, row[1] AS lat, row[2] AS lon\n" +
                            " MERGE (p:Point {id: id})\n" +
                            " SET p.lat = lat, p.long = lon, p.city ='Skopje'\n" +
                            " RETURN count(p) as skopje_nodes");
                }
                else if (file.equalsIgnoreCase("belgrade_nodes.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                            " WITH toInteger(row[0]) AS id, row[1] AS lat, row[2] AS lon\n" +
                            " MERGE (p:Point {id: id})\n" +
                            " SET p.lat = lat, p.long = lon, p.city ='Belgrade'\n" +
                            " RETURN count(p) as belgrade_nodes");
                }
                return result.single().toString();
            }
        });
        return res;
    }

    public static String loadWayEdges(Session session, String file){
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                if (file.equalsIgnoreCase("skopje_ways.csv")) {
                    result = tx.run("LOAD CSV FROM 'file:///" + file + "' AS row\n" +
                            " WITH toInteger(row[1]) AS point_id_1, toInteger(row[2]) AS point_id_2, row[0] AS way_id, toInteger(row[3]) AS weight\n" +
                            " MATCH (p1:Point {id: point_id_1})\n" +
                            " MATCH (p2:Point {id: point_id_2})\n" +
                            " MERGE (p1)-[rel:way]->(p2)\n" +
                            " SET rel.name = way_id, rel.weight = weight\n" +
                            " RETURN count(rel) as skopje_ways");
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

                return result.single().toString();
            }
        });
        return res;
    }

    public static String loadPathNodes(Session session, String file){
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                if (file.equalsIgnoreCase("skopje_paths_nodes.csv")){
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
                return result.single().toString();
            }
        });
        return res;
    }

    public static String loadTakesEdges(Session session, String file){
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                        " WITH toInteger(row[0]) AS user_id, toInteger(row[1]) AS path_id\n" +
                        " MATCH (u:User {id: user_id})\n" +
                        " MATCH (p:Path {id: path_id})\n" +
                        " MERGE (u)-[rel:takes]->(p)\n" +
                        " RETURN count(rel)");

                return result.single().toString();
            }
        });
        return res;
    }

    public static String loadPathToPointsEdges(Session session, String file){
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
                        " WITH toInteger(row[0]) AS path_id, toInteger(row[1]) AS start_point_id, toInteger(row[2]) AS end_point_id \n" +
                        " MATCH (p:Path {id: path_id})\n" +
                        " MATCH (ps:Point {id: start_point_id})\n" +
                        " MATCH (pe:Point {id: end_point_id})\n" +
                        " MERGE (p)-[rel1:start_trip_at]->(ps)\n" +
                        " MERGE (p)-[rel2:end_trip_at]->(pe)\n" +
                        " RETURN count(rel1), count(rel2)");

                return result.single().toString();
              }
            });
        return res;
    }

    // this method adds new way edges in order to connect disconnected subgraphs
    public static String loadAdditionalWays(Session session, String city) {
        EdgeCreator ec = new EdgeCreator();
        List<Tuple2<Long, Long>> pairs = new ArrayList<>(ec.generatePairs(session, city));

        String res = "";

        for (Tuple2<Long, Long> pair : pairs) {
            long num1 = pair._1;
            long num2 = pair._2;
            String id1 = String.valueOf(num1) + String.valueOf(num2);
            String id2 = String.valueOf(num2) + String.valueOf(num1);
            res = session.writeTransaction(new TransactionWork<String>() {
            Result result;

            @Override
            public String execute(Transaction tx) {
                result = tx.run("MATCH (p1:Point), (p2:Point)\n"
                        + "WHERE p1.id = " + num1 + " and p2.id = " + num2 + "\n"
                        + "MERGE (p1)-[w1:way]->(p2)\n"
                        + "MERGE (p2)-[w2:way]->(p1)\n"
                        + "SET w1.id = '" + id1 + "', w1.weight = 1000\n"
                        + "SET w2.id = '" + id2 + "', w2.weight = 1000\n"
                        + "RETURN count(w1), count(w2)");
                return result.single().toString();
            }
            });
        }
        return res;
    }

    public String loadNearEdges(String filename) {
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

    public void loadAllData(Session session){
        // LOAD USERS
        System.out.println(loadUsers(session, "skopje_users.csv"));
        System.out.println(loadUsers(session, "belgrade_users.csv"));

        // LOAD POINT NODES
        System.out.println(loadPointNodes(session, "skopje_nodes.csv"));
        System.out.println(loadPointNodes(session, "belgrade_nodes.csv"));

        // LOAD WAY EDGES
        System.out.println(loadWayEdges(session, "skopje_ways.csv"));
        System.out.println(loadWayEdges(session, "belgrade_ways.csv"));

        // LOAD PATH NODES
        System.out.println(loadPathNodes(session, "skopje_paths_nodes.csv"));
        System.out.println(loadPathNodes(session, "belgrade_paths_nodes.csv"));

        // LOAD TAKES EDGES
        System.out.println(loadTakesEdges(session, "skopje_takes.csv"));
        System.out.println(loadTakesEdges(session, "belgrade_takes.csv"));

        // LOAD PATH TO POINTS EDGES
        System.out.println(loadPathToPointsEdges(session, "belgrade_path_to_points.csv"));
        System.out.println(loadPathToPointsEdges(session, "skopje_path_to_points.csv"));

        // LOAD ADDITIONAL WAY EDGES
        System.out.println(loadAdditionalWays(session, "Belgrade"));
        System.out.println(loadAdditionalWays(session, "Skopje"));

        // LOAD NEAR EDGES
        System.out.println(loadNearEdges("belgrade_near_edges.csv"));
        System.out.println(loadNearEdges("skopje_near_edges.csv"));
  }

    public String naiveFindRideShareRecommendations(int requestingUserId, int pathId) {
        Map<String, Object> params = new HashMap<>();
        params.put("requestedPathId", pathId);
        params.put("requestingUserId", requestingUserId);
        String query =
                "MATCH (requestingUser: User {id: $requestingUserId})-[:takes]->(requestedPath: Path {id: $requestedPathId}),(requestedPath)-[:start_trip_at]->(requesterStartPoint), (requestedPath)-[:end_trip_at]->(requesterEndPoint)\n"
                        + "MATCH (offeredPath: Path)-[:start_trip_at]-()-[:near]->(requesterStartPoint), (offeredPath: Path)-[:end_trip_at]-()-[:near]->(requesterEndPoint)\n"
                        + "MATCH (offeringUser: User)-[:takes]->(offeredPath)\n"
                        + "WHERE requestingUser <> offeringUser AND ABS(duration.between(time(requestedPath.time_of_day), time(offeredPath.time_of_day)).minutes) <= 30\n"
                        + "RETURN offeringUser, offeredPath";
        List<Record> records =
                session.readTransaction(
                        (tx) -> {
                            Result result = tx.run(query, params);
                            return result.list();
                        });
        if (records.isEmpty()) {
            return "We have no recommendations for you now, check back later";
        }
        return records.stream()
                .map(
                        record -> {
                            Value offeringUser = record.get("offeringUser");
                            Value offeredPath = record.get("offeredPath");
                            String offeringUserName = offeringUser.get("name").asString();
                            String offeringCarStatus = offeringUser.get("car_owner").asString();
                            StringBuilder userResponse = new StringBuilder().append(offeringUserName);
                            if (offeringCarStatus.equals("True"))
                                userResponse.append(" owns a car ");
                            userResponse.append(" and takes a similar path as you and starts at ");
                            userResponse.append(offeredPath.get("time_of_day").asString());
                            return userResponse.toString();
                        }).collect(joining("\n"));
    }

    public String betweenness(Session session) {
        String res = session.writeTransaction(new TransactionWork<String>() {
            Result result;

            @Override
            public String execute(Transaction tx) {
                result = tx.run("CALL algo.betweenness.stream('Point','way',{direction:'out'})\n"
                        + "YIELD nodeId, centrality\n"
                        + "MATCH (point:Point) WHERE id(point) = nodeId\n"
                        + "RETURN point.id AS p,centrality\n"
                        + "ORDER BY centrality DESC");
                return result.list(res -> res.values()).toString();
            }
        });
        return res;
    }

//  public String findRideShareRecommendations(String pathId) {
//    Map<String, Object> params = new HashMap<>();
//    params.put("pathId", pathId);
//    String query =
//        "MATCH (requestedPath: Path {id: $pathId}),(requestedPath)-[:start_trip_at]->(requesterStartPoint), (requestedPath)-[:end_trip_at]->(requesterEndPoint)\n"
//            + "MATCH (offeredPath: Path)-[:start_trip_at]-()-[:near]->(requesterStartPoint), (offeredPath: Path)-[:end_trip_at]-()-[:near]->(requesterEndPoint)\n"
////            + "WHERE abs(offeredPath.time_of_day - requestedPath.time_of_day) <= 20\n"
//            + "MATCH (offeringUser: User)-[:takes]->(offeredPath)\n"
//            + "RETURN offeringUser, offeredPath";
//
//      List<Record> list = session.readTransaction((tx) -> tx.run(query, params)).list();
//      return list.toString();
//  }
}

//    String pageRank = "CALL algo.pageRank.stream('Point', 'way', {iterations:20, dampingFactor:0.70})\n" +
//            "YIELD nodeId, score\n" +
//            "wHERE algo.asNode(nodeId).city=\"Belgrade\"\n" +
//            "RETURN algo.asNode(nodeId).id AS point,score\n" +
//            "ORDER BY score DESC";