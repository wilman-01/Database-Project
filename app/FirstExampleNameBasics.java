package app;

import java.io.BufferedReader;
import java.io.FileReader;
//STEP 1. Import required packages
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class FirstExampleNameBasics {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "org.postgresql.Driver";  
   static final String DB_URL = "jdbc:postgresql://db-315.cse.tamu.edu/rmyoung_db";

   //  Database credentials
   static final String USER = "lwooler";
   static final String PASS = "studentpwd";
   
   public static void main(String[] args) {
   Connection conn = null;
   Statement stmt = null;
   String sql = null;
   try {
      //STEP 2: Register JDBC driver
      Class.forName("org.postgresql.Driver");

      //STEP 3: Open a connection
      System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);
      System.out.println("Opened database successfully");
      
      tsvParserNameBasics p = new tsvParserNameBasics(sql, conn, stmt);

      conn.close();
   } catch(SQLException se) {
      //Handle errors for JDBC
      se.printStackTrace();
   } catch(Exception e) {
      //Handle errors for Class.forName
      e.printStackTrace();
   } finally {
      //finally block used to close resources
      try {
         if(stmt!=null)
            stmt.close();
      } catch(SQLException se2) {}// nothing we can do

      try {
         if(conn!=null)
            conn.close();
      } catch(SQLException se) {
         se.printStackTrace();
      }//end finally try
   }//end try
   System.out.println(" Goodbye!");
}//end main
}//end FirstExample