package com.cottagecoders.oracleinserts;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class OracleInserts {

  private static final String URL = "jdbc:oracle:thin:@localhost:1521:ORCLCDB";
  private static final String USERNAME = System.getProperty("username");
  private static final String PASSWORD = System.getProperty("password");
  private static final int RECORD_COUNT = 3000;
  private static final int FIELDS_PER_RECORD = 3;

  public static void main(String... args) throws SQLException {

    if (StringUtils.isEmpty(USERNAME)) {
      help();
    }
    if (StringUtils.isEmpty(URL)) {
      help();
    }
    if (StringUtils.isEmpty(PASSWORD)) {
      help();
    }

    OracleInserts oi = new OracleInserts();
    try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
      Statement stmt = conn.createStatement();
      try {
        dropTable(stmt);
      } catch (SQLException ex) {
        System.out.println("drop table exception " + ex.getMessage());
      }
      createTable(stmt);

      boolean multiRow = true;
      if (multiRow) {
        oi.multiRowOperation(conn);
      } else {
        oi.singleRowOperation(conn);
      }
    }
  }

  private static void help() {
    System.out.println("Run it like this:");
    System.out.println("Note - the URL is currently hardcoded.");
    System.out.println(
        "java  -Dusername=user -Dpassword=pass  -jar target/OracleInserts-1.0-SNAPSHOT-jar-with-dependencies.jar");
    System.exit(3);
  }

  private void multiRowOperation(Connection conn) throws  SQLException {

    // build the statement.
    String sql = "INSERT ALL ";
    for (int i = 0; i < RECORD_COUNT; i++) {
      sql += "INTO bob5 VALUES (?, ?, ?) ";
    }
    sql += "SELECT 1 FROM dual";

    int batch = 0;
    // with the set-up done, let's loop, inserting records.
    for (int j = 0; j < 100000; j++) {
      long start = System.nanoTime();

      PreparedStatement ps = conn.prepareStatement(sql);
      for (int i = 0; i < RECORD_COUNT * FIELDS_PER_RECORD; i += 3) {
        int id = i + batch;
        ps.setInt(i+1, id);
        ps.setString(i + 2 , UUID.randomUUID().toString());
        ps.setString(i + 3, UUID.randomUUID().toString());
      }
      batch += RECORD_COUNT +1;
      System.out.print("batch " + batch);

      ps.execute();
      ps.close();
      long tt = System.nanoTime() - start;
      System.out.println("record elapsed " + tt + "ns  " +  tt/1_000_000 + "ms");

    }
}

  private void singleRowOperation(Connection conn) throws SQLException {

    // build the statement.
    String sql = "INSERT INTO bob5 (id, fname, lname) VALUES (?, ?, ?)";

    // with the set-up done, let's loop, inserting records.
    int batch = 0;
    for (int k = 1 ; k < 10000; k++) {
      long elapsed = 0;
      for (int j = 0; j < RECORD_COUNT; j++) {
        long start = System.nanoTime();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, j + batch);
        ps.setString(2, UUID.randomUUID().toString());
        ps.setString(3, UUID.randomUUID().toString());
        ps.execute();
        ps.close();
        elapsed += System.nanoTime() - start;
      }
      batch += k * RECORD_COUNT;
      System.out.println("single mode.  3000 records elapsed " + elapsed + "ns  " +  elapsed/1_000_000 + "ms");

    }
    //      selectAll(stmt);

}

  static void dropTable(Statement stmt) throws SQLException {
    stmt.execute("DROP TABLE bob5");
  }

  static void createTable(Statement stmt) throws SQLException {
    String sql = "CREATE TABLE bob5 (id NUMBER PRIMARY KEY, fname VARCHAR(50), lname VARCHAR(50))";
    stmt.execute(sql);
    System.out.println("created table:  " + sql);
  }

  void selectAll(Statement stmt) throws SQLException {
    System.out.println("selecting from table:");
    ResultSet rs = stmt.executeQuery("SELECT * FROM bob5  ORDER BY id");
    while (rs.next()) {
      System.out.println(rs.getInt(1) + "  " + rs.getString(2) + " " + rs.getString(3));
    }
  }
}
