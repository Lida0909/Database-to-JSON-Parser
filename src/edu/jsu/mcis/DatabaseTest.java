package edu.jsu.mcis;

import java.sql.*;
import java.util.ArrayList;
import org.json.simple.*;

public class DatabaseTest {

    public static void main(String[] args) {
        
        JSONArray array = (JSONArray) getJSONData();
        System.out.println("\nConversion results from Database into an array of JSON objects");
        System.out.println("*************************************************************");
        System.out.println(array);
        System.out.println();
    }
    
    public static JSONArray getJSONData(){
        
        Connection conn = null;
        PreparedStatement pstSelect = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        JSONArray list = new JSONArray();
        String query, value;
        
        ArrayList<String> records = new ArrayList<>();
        
        boolean hasresults;
        int resultCount, columnCount;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS448";
            
            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            /* Open Connection */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!");
                
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                /* Execute Select Query */
                
                hasresults = pstSelect.execute();                
                
                /* Get Results */
                
                System.out.println("Getting Results ...");
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
                        /* Get Column Names; Append them in an ArrayList "key" */
                        
                        for (int i = 2; i <= columnCount; i++) {
                            records.add(metadata.getColumnLabel(i));
                        }
                        
                        /* Get Data; put the data in JSONObject */
                        
                        while(resultset.next()) {
                            
                            /* Begin Next ResultSet Row; Loop Through ResultSet
                            Colums; Append to jsonObject*/
                            
                            JSONObject object = new JSONObject();

                            for (int i = 2; i <= columnCount; i++) {
                                
                                JSONObject jsonObject = new JSONObject();
                                value = resultset.getString(i);

                                if (resultset.wasNull()) {
                                    jsonObject.put(records.get(i-2), "NULL");
                                    jsonObject.toJSONString();
                                }

                                else {
                                    jsonObject.put(records.get(i-2), value);
                                    jsonObject.toJSONString();
                                }
                                object.putAll(jsonObject);
                            }
                            list.add(object);
                        }
                        
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstSelect.getMoreResults();

                }
                
            }
            
            /* Close Database Connection */
            
            conn.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
                        
        }
        return list;
        
    }
    
}