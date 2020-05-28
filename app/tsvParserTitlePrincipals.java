package app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class tsvParserTitlePrincipals {

    public tsvParserTitlePrincipals(String sql, Connection conn, Statement stmt) throws Exception {

        StringTokenizer st;
        StringTokenizer st2;
        String titleID;
        String personID;
        String category;
        String job;
        String characters;
        
        BufferedReader TSVFile = new BufferedReader(new FileReader("title.principals.tsv"));
        String dataRow = TSVFile.readLine(); // Read first line.
        dataRow = TSVFile.readLine(); // Read second line.
        
        stmt = conn.createStatement();
        
        int counter = 0;
        while (dataRow != null) {
            st = new StringTokenizer(dataRow,"\t");

            titleID = st.nextElement().toString();

            // orderings
            try {
               st.nextElement();
            } catch (Exception e) {}

            personID = st.nextElement().toString();

            // if this throws an error, it's because the value is blank
            try {
                category = st.nextElement().toString();

                if (category.charAt(0) == '\\')
                  category = "NULL";
                else {
                     for (int i = 0; i < category.length(); i++) {
                     if (category.charAt(i) == '\'') {
                        category = category.substring(0, i+1) + '\'' + category.substring(i+1);
                        i += 1;
                     }
                   }
                } 
            } catch (Exception e) {
                category = "NULL";
            }
            
            // if this throws an error, it's because the value is blank
            try {
                job = st.nextElement().toString();

                if (job.charAt(0) == '\\')
                  job = "NULL";
                else {
                   for (int i = 0; i < job.length(); i++) {
                     if (job.charAt(i) == '\'') {
                        job = job.substring(0, i+1) + '\'' + job.substring(i+1);
                        i += 1;
                     }
                   }
                } 
            } catch (Exception e) {
                job = "NULL";
            }

            try {
               characters = st.nextElement().toString();

               if (characters.charAt(0) == '\\')
                  characters = "NULL";
               else {
                  // parse out the brackets
                  characters = characters.substring(1, characters.length() - 1);
               }
            } catch (Exception e) {
                characters = "NULL";
            }

            st2 = new StringTokenizer(characters,",");

            sql = "INSERT INTO \"IMDb\".title_principals (titleID,personID,category,job) VALUES ('"+titleID+"','"+personID+"','"+category+"','"+job+"');";
            
            stmt.addBatch(sql);

            if (characters != "NULL") {
               st2 = new StringTokenizer(characters,",");

               String character;
               while (st2.hasMoreElements()) {
                  character = st2.nextElement().toString();

                  // parse out quotes
                  if (character.length() > 2)
                     character = character.substring(1, character.length() - 1);

                  for (int i = 0; i < character.length(); i++) {
                     if (character.charAt(i) == '\'') {
                        character = character.substring(0, i+1) + '\'' + character.substring(i+1);
                        i += 1;
                     }
                  }
                  
                  sql = "INSERT INTO \"IMDb\".character (titleID,personID,character) VALUES ('"+titleID+"','"+personID+"','"+character+"');";
                  stmt.addBatch(sql);
               }
            }

            if (counter % 100000 == 0)
               stmt.executeBatch();

            counter += 1;

            dataRow = TSVFile.readLine(); // Read next line of data.
        }
        stmt.executeBatch();
        // Close the file once all data has been read.
        TSVFile.close();

        // End the printout with a blank line.
        System.out.println();

    } //main()
} // TSVRead