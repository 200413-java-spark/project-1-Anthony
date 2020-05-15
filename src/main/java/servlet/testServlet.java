package servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import io.SQLIo;
import io.SQLSource;

public class testServlet extends HttpServlet {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void init() {

  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.getWriter().println("Hello World");
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String[] request = req.getParameterValues("table")[0].split(",");
    PrintWriter output = resp.getWriter();

    output.print("<h1>Anthony's Project </h1><br>");
    // output.print(request[0] + request[1]);
    // SQLIo sqlCommands = new SQLIo();
    if (request[0].equals("sparkTableDiff")) {
      output.println(
          "| Name        |  SC Price   |   HC Price   |    Diff    |    Percent Diff SC    |    Percent Diff HC   |<br>");

      try {

        String[] list = SQLIo.readSQL(request[0], request[1], true).toString().split(",");
        for (int i = 0; i < list.length; i++) {
          output.println(list[i]);
          output.println("<br>");
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } else if (request[0].equals("sparkTableAvg") || request[0].equals("sparkTableHCAvg")) {
      output.println("| Name        |  Price    |<br>");
      try {
        String[] list = SQLIo.readSQL(request[0], request[1]).toString().split(",");
        for (int i = 0; i < list.length; i++) {
          output.println(list[i]);
          output.println("<br>");
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } else {
      try {
        String[] list = SQLIo.readSQL(request[0]).toString().split(",");
        output.println(
            "| Name        |  SC Price   |   HC Price   |    Diff    |    Percent Diff SC   |    Percent Diff HC   |<br>");
        for (int i = 0; i < list.length; i++) {
          output.println(list[i]);
          output.println("<br>");
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}