package app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class tsvParserTitleEpisode {

    public tsvParserTitleEpisode(String sql, Connection conn, Statement stmt) throws Exception {

        StringTokenizer st;
        String titleID;
        String parentID;
        int seasonNum;
        int episodeNum;
        
        BufferedReader TSVFile = new BufferedReader(new FileReader("title.episode.tsv"));
        String dataRow = TSVFile.readLine(); // Read first line.
        dataRow = TSVFile.readLine(); // Read first line.
        
        stmt = conn.createStatement();
        
        int counter = 0;
        while (dataRow != null) {
            st = new StringTokenizer(dataRow,"\t");

            titleID = st.nextElement().toString();
            parentID = st.nextElement().toString();

            // if this throws an error, it's because the value is "\N"
            try {
                seasonNum = Integer.parseInt(st.nextElement().toString());
            } catch (Exception e) {
                seasonNum = 0;
            }

            // if this throws an error, it's because the value is "\N"
            try {
                episodeNum = Integer.parseInt(st.nextElement().toString());
            } catch (Exception e) {
                episodeNum = 0;
            }
            
            if (seasonNum == 0 && episodeNum == 0) {
                sql = "INSERT INTO \"IMDb\".title_episode (titleID,parentID,seasonNum,episodeNum) VALUES ('"+titleID+"','"+parentID+"',NULL,NULL);";
            }
            else if (seasonNum == 0) {
                sql = "INSERT INTO \"IMDb\".title_episode (titleID,parentID,seasonNum,episodeNum) VALUES ('"+titleID+"','"+parentID+"',NULL,"+episodeNum+");";
            }
            else if (episodeNum == 0) {
                sql = "INSERT INTO \"IMDb\".title_episode (titleID,parentID,seasonNum,episodeNum) VALUES ('"+titleID+"','"+parentID+"',"+seasonNum+",NULL);";
            }
            else {
                sql = "INSERT INTO \"IMDb\".title_episode (titleID,parentID,seasonNum,episodeNum) VALUES ('"+titleID+"','"+parentID+"',"+seasonNum+","+episodeNum+");";
            }

            stmt.addBatch(sql);

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