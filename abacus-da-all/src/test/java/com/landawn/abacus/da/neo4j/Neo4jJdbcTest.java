package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// https://github.com/neo4j-contrib/neo4j-jdbc
public class Neo4jJdbcTest {

    @BeforeAll
    public static void setUpClass() throws IOException {
        try {
            Class.forName("org.neo4j.jdbc.Driver");
        } catch (final ClassNotFoundException e) {
            assumeTrue(false, "Neo4j JDBC integration tests require the optional org.neo4j.jdbc.Driver");
        }

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 7474), 1000);
        } catch (final ConnectException | SocketTimeoutException e) {
            assumeTrue(false, "Live Neo4j is unavailable at localhost:7474: " + e.getMessage());
        }
    }

    /**
     * 
     *
     * @throws Exception 
     */
    @Test
    public void test_01() throws Exception {
        try (Connection con = DriverManager.getConnection("jdbc:neo4j://localhost:7474/", "neo4j", "admin");
             Statement stmt = con.createStatement()) {
            stmt.executeUpdate("CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})");

            try (ResultSet rs = stmt.executeQuery("MATCH (cloudAtlas {title: \"Cloud Atlas\"})<-[:DIRECTED]-(directors) RETURN directors.name")) {
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
        }
    }

}
