package com.withjoy;

import java.sql.*;
import java.util.*;

/**
 *
 * @author akiramadono
 */
public class ReadPostgreSQL {

    public ReadPostgreSQL() {
    }

    /**
     * Setup a connection to postgres database
     *
     * @return
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        Properties sql_properties = new AcquireProperties("properties.txt").getProperties();
        String hostName = sql_properties.getProperty("pg_host");
        String port = sql_properties.getProperty("pg_port");
        String database = sql_properties.getProperty("pg_db");
        String userName = sql_properties.getProperty("pg_user");
        String passWord = sql_properties.getProperty("pg_pass");

        String url = "jdbc:postgresql://" + hostName + ":" + port + "/" + database + "?user=" + userName + "&password=" + passWord;
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }

    /**
     * Generate diagnostics for errors in SQL connection
     *
     * @param sqle SQLException object
     */
    public void printSQLException(SQLException sqle) {
        System.err.println("\n---SQLException Caught---\n");
        System.err.println("SQLState: " + sqle.getSQLState());
        System.err.println("Severity: " + sqle.getErrorCode());
        System.err.println("Message: " + sqle.getMessage());
        sqle.printStackTrace();
        sqle = sqle.getNextException();
    }

    /**
     * Generate hash map between column names and ordering from specific SQL
     * Query
     *
     * @param query
     * @return         HashMap between column names and ordering
     */
    public HashMap<String, Integer> getSQLHash(String query) {
        HashMap<String, Integer> hash = new HashMap<>();
        try (Connection input = getConnection()){
            PreparedStatement prepared_statement = input.prepareStatement(query);
            ResultSet result_set = prepared_statement.executeQuery();
            ResultSetMetaData result_metadata = result_set.getMetaData();
            int columnCount = result_metadata.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                hash.put(result_metadata.getColumnName(i), (Integer) i);
            }
            input.close();
        } catch (SQLException sqle) {
            printSQLException(sqle);
        }
        return hash;
    }
    
    public HashMap<String,Integer> GetTableData(String table) {
        Properties properties = new AcquireProperties("properties.txt").getProperties();
        String schema = properties.getProperty("pg_schema");
        return new HashMap<>(
                getSQLHash("SELECT * FROM " + schema + "." + table + " LIMIT 1"));        
    }
    
}
