package com.cottagecoders.oracleinserts;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleInserts {

  private static final String url = "jdbc:oracle:thin:@localhost:1521:ORCLCDB";
  private static final String userName = System.getProperty("username");
  private static final String password = System.getProperty("password");

  public static void main(String... args) {

    OracleInserts oi = new OracleInserts();
    oi.run();

  }

  private void help() {
    System.out.println("Run it like this:");
    System.out.println("java  -Dusername=user -Dpassword=pass  -jar target/OracleInserts-1.0-SNAPSHOT-jar-with-dependencies.jar");
    System.exit(3);
  }

  private void run() {

    if (StringUtils.isEmpty(userName)) {
      help();
    }
    if (StringUtils.isEmpty(password)) {
      help();
    }

    System.out.println("start.");
    try (Connection conn = DriverManager.getConnection(url, userName, password)) {
      Statement stmt = conn.createStatement();


      try {
        dropTable(stmt);
      } catch (SQLException ex) {
        System.out.println("drop table exception " + ex.getMessage());
      }
      createTable(stmt);

      // build the statement.
      String sql = "INSERT ALL ";
      sql += "INTO bob5 VALUES (?, ?, ?) ";
      sql += "INTO bob5 VALUES (?, ?, ?) ";
      sql += "INTO bob5 VALUES (?, ?, ?) ";
      sql += "SELECT 1 FROM dual";

      // "normal" syntax for MySQL, for example:
      // INSERT INTO bob5 (id, fname, lname) VALUES (?,?,?), (?,?,?), (?,?,?)

      System.out.println("SQL " + sql);
      PreparedStatement ps = conn.prepareStatement(sql);
      long one = 11L;
      String a = "a";
      ps.setLong(1, one);
      ps.setString(2, a);
      ps.setString(3, a);

      long two = 12l;
      String b = "bb";
      ps.setLong(4, two);
      ps.setString(5, b);
      ps.setString(6, b);


      long three = 13L;
      String c = "cc";
      ps.setLong(7, three);
      ps.setString(8, c);
      ps.setString(9, c);

      ps.execute();

      selectAll(stmt);
    } catch (SQLException ex) {
      System.out.println("Exception: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

  void dropTable(Statement stmt) throws SQLException {
    stmt.execute("DROP TABLE bob5");
  }

  void createTable(Statement stmt) throws SQLException {
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
