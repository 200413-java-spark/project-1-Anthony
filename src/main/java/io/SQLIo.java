package io;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import items.UniqueItem;
import scala.Tuple2;

public class SQLIo {
  public static void insertSQL(List<UniqueItem> items, String tableName) throws SQLException {
    // Date newDate = Date.valueOf(item.getDate());
    try (Connection conn = SQLSource.getConnection();

    ) {
      Statement statement = conn.createStatement();
      conn.setAutoCommit(false);

      statement.execute("DROP TABLE IF EXISTS " + tableName + " CASCADE");
      statement.execute("CREATE TABLE " + tableName
          + " (id serial primary key, league varchar, date date, itemID integer, type varchar, name varchar, baseType varchar, value decimal)");
      PreparedStatement rawDStatement = conn.prepareStatement(
          "INSERT INTO " + tableName + " (league, date, itemID, type, name, baseType, value) VALUES(?,?,?,?,?,?,?)");
      for (int i = 0; i < items.size(); i++) {
        // Date newDate = Date.valueOf(items.get(i).getDate());
        rawDStatement.setString(1, items.get(i).getLeague());
        rawDStatement.setDate(2, Date.valueOf(items.get(i).getDate()));
        rawDStatement.setInt(3, items.get(i).getItemID());
        rawDStatement.setString(4, items.get(i).getType());
        rawDStatement.setString(5, items.get(i).getName());
        rawDStatement.setString(6, items.get(i).getBaseType());
        rawDStatement.setDouble(7, items.get(i).getValue());
        rawDStatement.addBatch();
      }
      try {
        rawDStatement.executeBatch();
      } catch (SQLException e) {
        System.out.println("Error message: " + e.getMessage());
        return;
      }

      conn.commit();

      ResultSet rs = null;
      rs = statement.executeQuery("SELECT * FROM " + tableName + " ORDER BY id DESC LIMIT 20");
      while (rs.next()) {
        System.out.println(rs.getString("name"));
      }
    }
  }

  public static void insertAverages(List<Tuple2<String, Double>> items, String tableName) throws SQLException {
    // String tableName = "sparkTableHC";
    try (Connection conn = SQLSource.getConnection();

    ) {
      Statement statement = conn.createStatement();
      conn.setAutoCommit(false);

      statement.execute("DROP TABLE IF EXISTS " + tableName + "AVG CASCADE");
      statement.execute("CREATE TABLE " + tableName + "AVG (id serial primary key, name varchar, value decimal)");
      PreparedStatement rawDStatement = conn
          .prepareStatement("INSERT INTO " + tableName + "AVG (name, value) VALUES(?,?)");
      for (int i = 0; i < items.size(); i++) {
        // Date newDate = Date.valueOf(items.get(i).getDate());
        rawDStatement.setString(1, items.get(i)._1);
        rawDStatement.setDouble(2, items.get(i)._2);
        rawDStatement.addBatch();
      }
      try {
        rawDStatement.executeBatch();
      } catch (SQLException e) {
        System.out.println("Error message: " + e.getMessage());
        return;
      }

      conn.commit();

      ResultSet rs = null;
      rs = statement.executeQuery("SELECT * FROM " + tableName + "AVG ORDER BY id DESC LIMIT 20");
      while (rs.next()) {
        System.out.println(rs.getString("name"));
      }
    }
  }
}