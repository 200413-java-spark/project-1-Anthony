package io;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;

import items.UniqueItem;

public class SQLIo {
  public static void insertSQL(UniqueItem item) throws SQLException {
    Date newDate = Date.valueOf(item.getDate());
    try (Connection conn = SQLSource.getConnection();) {
      PreparedStatement rawDStatement = conn.prepareStatement(
          "insert into sparktable (league, date, itemID, type, name, baseType, value) values(?,?,?,?,?,?,?)");
      rawDStatement.setString(1, item.getLeague());
      rawDStatement.setDate(2, newDate);
      rawDStatement.setInt(3, item.getItemID());
      rawDStatement.setString(4, item.getType());
      rawDStatement.setString(5, item.getName());
      rawDStatement.setString(6, item.getBaseType());
      rawDStatement.setDouble(7, item.getValue());
      rawDStatement.addBatch();
      rawDStatement.executeBatch();
    }
  }
}