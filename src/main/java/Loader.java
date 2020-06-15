import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

public class Loader {
    public static String executeTransaction(Session session, String file){
        /* Before uploading to Nao4j do:
            CREATE (node:User), (node:Point)
            CREATE CONSTRAINT ON (p:User) ASSERT p.id IS UNIQUE;
            CREATE CONSTRAINT ON (p:Point) ASSERT p.id IS UNIQUE;
            */
        String res = session.writeTransaction(new TransactionWork<String>()
        {
            Result result;
            @Override
            public String execute( Transaction tx )
            {
                if (file.equalsIgnoreCase("users.csv")){
                    result = tx.run( "LOAD CSV WITH HEADERS FROM 'file:///" + file + "' AS row\n" +
                            "WITH toInteger(row.ID) AS id, row.Name AS name, row.CarOwner AS car_owner\n" +
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
                else if (file.equalsIgnoreCase("skopje_ways.csv"))
                {
                    result = tx.run( "LOAD CSV FROM 'file:///" + file +"' AS row\n" +
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
                return result.single().toString();
            }
        } );
        return res;
    }
}
