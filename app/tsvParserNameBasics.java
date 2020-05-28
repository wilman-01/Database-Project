package app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class tsvParserNameBasics {

    public tsvParserNameBasics(String sql, Connection conn, Statement stmt) throws Exception {

        StringTokenizer st;
        StringTokenizer st2;
        StringTokenizer st3;
        String nconst;
        String primaryName;
        int birthYear;
        int deathYear;
        String primaryProfession;
        String profession;
        String title;
        String knownForTitles;
        
        BufferedReader TSVFile = new BufferedReader(new FileReader("name.basics.tsv"));
        String dataRow = TSVFile.readLine(); // Read first line.
        dataRow = TSVFile.readLine(); // Read first line.
        
        stmt = conn.createStatement();
        
        int counter = 0;
        while (dataRow != null) {
            st = new StringTokenizer(dataRow,"\t");

            nconst = st.nextElement().toString();
            primaryName = st.nextElement().toString();

            // checking for apostaphes in person's name
            for (int i = 0; i < primaryName.length(); i++) {
                if (primaryName.charAt(i) == '\'') {
                    primaryName = primaryName.substring(0, i+1) + '\'' + primaryName.substring(i+1);
                    i += 1;
                }
            }

            // if this throws an error, it's because the value is "\N"
            try {
                birthYear = Integer.parseInt(st.nextElement().toString());
            } catch (Exception e) {
                birthYear = 0;
            }

            // if this throws an error, it's because the value is "\N"
            try {
                deathYear = Integer.parseInt(st.nextElement().toString());
            } catch (Exception e) {
                deathYear = 0;
            }

            // if this throws an error, it's because the value is blank
            try {
                primaryProfession = st.nextElement().toString();
            } catch (Exception e) {
                primaryProfession = "NULL";
            }
            
            // if this throws an error, it's because the value is blank
            try {
                knownForTitles = st.nextElement().toString();
            } catch (Exception e) {
                knownForTitles = "NULL";
            }
            
            st2 = new StringTokenizer(primaryProfession,",");
            st3 = new StringTokenizer(knownForTitles,",");
            
            if (birthYear == 0 && deathYear == 0) {
                sql = "INSERT INTO \"IMDb\".name_basics (PersonID,Name,BirthYear,DeathYear) VALUES ('"+nconst+"','"+primaryName+"',NULL,NULL);";
            }
            else if (birthYear == 0) {
                sql = "INSERT INTO \"IMDb\".name_basics (PersonID,Name,BirthYear,DeathYear) VALUES ('"+nconst+"','"+primaryName+"',NULL,"+deathYear+");";
            }
            else if (deathYear == 0) {
                sql = "INSERT INTO \"IMDb\".name_basics (PersonID,Name,BirthYear,DeathYear) VALUES ('"+nconst+"','"+primaryName+"',"+birthYear+",NULL);";
            }
            else {
                sql = "INSERT INTO \"IMDb\".name_basics (PersonID,Name,BirthYear,DeathYear) VALUES ('"+nconst+"','"+primaryName+"',"+birthYear+","+deathYear+");";
            }
            
            stmt.addBatch(sql);
        
            while (st2.hasMoreElements()) {
                profession = st2.nextElement().toString();
                
                sql = "INSERT INTO \"IMDb\".profession (PersonID,Profession) VALUES ('"+nconst+"','"+profession+"');";
                stmt.addBatch(sql);
            }
            
            while (st3.hasMoreElements()) {
                title = st3.nextElement().toString();
                
                sql = "INSERT INTO \"IMDb\".KFTitles (PersonID,Titles) VALUES ('"+nconst+"','"+title+"');";
                stmt.addBatch(sql);
            }

            counter += 1;

            if (counter % 100000 == 0)
                stmt.executeBatch();

            dataRow = TSVFile.readLine(); // Read next line of data.
        }
        stmt.executeBatch();
        // Close the file once all data has been read.
        TSVFile.close();

        // End the printout with a blank line.
        System.out.println();

    } //main()
} // TSVRead