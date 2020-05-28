package app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class tsvParserTitleAkas{

    public tsvParserTitleAkas(String sql, Connection conn, Statement stmt) throws Exception {

        StringTokenizer st ;
        StringTokenizer st2 ;
        StringTokenizer st3 ;
        String title_id;
        String title;
        String region;
        String language;
        String type;
        String attributes;
        
        BufferedReader TSVFile = new BufferedReader(new FileReader("title.akas.tsv"));
        String dataRow = TSVFile.readLine(); // Read first line.
        dataRow = TSVFile.readLine(); // Read first line.
        
        stmt = conn.createStatement();
		int counter = 1;
        while (dataRow != null) {
            st = new StringTokenizer(dataRow,"\t");

            title_id = st.nextElement().toString();

			try {
				st.nextElement();
			} catch (Exception e) {}

			title = st.nextElement().toString();
			if (title.charAt(0) == '\\')
					title = "NULL";
			else{
				for (int i = 0; i < title.length(); i++) {
					if (title.charAt(i) == '\'') {
						title = title.substring(0, i+1) + '\'' + title.substring(i+1);
						i += 1;
					}
				}
			}

            // if this throws an error, it's because the value is "\N"
            try {
                region = st.nextElement().toString();
				if (region.charAt(0) == '\\')
					region = "NULL";
            } catch (Exception e) {
                region = "NULL";
            }

            // if this throws an error, it's because the value is blank
            try {
                language = st.nextElement().toString();
				if (language.charAt(0) == '\\')
					language = "NULL";
            } catch (Exception e) {
                language = "NULL";
            }
            
            // if this throws an error, it's because the value is blank
            try {
                type = st.nextElement().toString();
				if (type.charAt(0) == '\\')
					type = "NULL";
				else {
					for (int i = 0; i < type.length(); i++) {
						if (type.charAt(i) == '\'') {
							type = type.substring(0, i+1) + '\'' + type.substring(i+1);
							i += 1;
						}
					}
					}
            } catch (Exception e) {
                type = "NULL";
            }

			// if this throws an error, it's because the value is blank
            try {
                attributes = st.nextElement().toString();
				if (attributes.charAt(0) == '\\')
					attributes = "NULL";
				else {
					for (int i = 0; i < attributes.length(); i++) {
						if (attributes.charAt(i) == '\'') {
							attributes = attributes.substring(0, i+1) + '\'' + attributes.substring(i+1);
							i += 1;
						}
					}
				}
            } catch (Exception e) {
                attributes = "NULL";
            }

			try {
				st.nextElement();
			} catch (Exception e) {}
               
            sql = "INSERT INTO \"IMDb\".title_akas (titleID,title,region,language,types,attributes) VALUES ('"+title_id+"','"+title+"','"+region+"','"+language+"','"+type+"','"+attributes+"');";
            //System.out.println(sql);
            stmt.addBatch(sql);
            dataRow = TSVFile.readLine(); // Read next line of data.
			counter = counter+1;
			if (counter % 100000 == 0) {
				stmt.executeBatch();
			}
        }
		stmt.executeBatch();

        // Close the file once all data has been read.
        TSVFile.close();

        // End the printout with a blank line.
        System.out.println();

    } //main()
} // TSVRead