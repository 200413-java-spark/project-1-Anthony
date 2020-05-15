package io;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import items.UniqueItem;

public class SQLIo {
  public static void insertSQL(List<UniqueItem> items) throws SQLException {
    // Date newDate = Date.valueOf(item.getDate());
    try (Connection conn = SQLSource.getConnection();

    ) {
      Statement statement = conn.createStatement();
      conn.setAutoCommit(false);

      statement.execute("DROP TABLE IF EXISTS sparktable CASCADE");
      statement.execute(
          "CREATE TABLE sparktable(id serial primary key, league varchar, date date, itemID integer, type varchar, name varchar, baseType varchar, value decimal)");
      PreparedStatement rawDStatement = conn.prepareStatement(
          "INSERT INTO sparktable (league, date, itemID, type, name, baseType, value) VALUES(?,?,?,?,?,?,?)");
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
      rs = statement.executeQuery("SELECT * FROM sparktable ORDER BY id DESC LIMIT 20");
      while (rs.next()) {
        System.out.println(rs.getString("name"));
      }
      // rawDStatement.setString(1, item.getLeague());
      // rawDStatement.setDate(2, newDate);
      // rawDStatement.setInt(3, item.getItemID());
      // rawDStatement.setString(4, item.getType());
      // rawDStatement.setString(5, item.getName());
      // rawDStatement.setString(6, item.getBaseType());
      // rawDStatement.setDouble(7, item.getValue());
      // rawDStatement.addBatch();
      // rawDStatement.executeBatch();
    }
  }
}