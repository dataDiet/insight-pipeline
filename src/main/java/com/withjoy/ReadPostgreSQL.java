package com.withjoy;
 
import java.sql.*;
import java.util.*;

/**
 *
 * @author akiramadono
 */

public class ReadPostgreSQL {

	public ReadPostgreSQL(){}

	public static Connection getConnection() throws SQLException{
                Properties sql_properties = new AcquireProperties("properties.txt").getProperties();
                String hostName = sql_properties.getProperty("pg_host");
                String port = sql_properties.getProperty("pg_port");
                String database = sql_properties.getProperty("pg_db");
                String userName = sql_properties.getProperty("pg_user");
                String passWord = sql_properties.getProperty("pg_pass");
                
		String url = "jdbc:postgresql://"+hostName+":"+port+"/"+database+"?user="+userName+"&password="+passWord;
		System.out.println(url);
		Connection conn = DriverManager.getConnection(url);
		return conn;
	}

	public static void printSQLException(SQLException sqle){
		System.out.println("\n---SQLException Caught---\n");
		System.out.println("SQLState: " + sqle.getSQLState());
		System.out.println("Severity: " + sqle.getErrorCode());
		System.out.println("Message: " + sqle.getMessage());
		sqle.printStackTrace();
		sqle = sqle.getNextException();
	}

	public static HashMap<String, Integer> getSQLHash(String query){
		HashMap<String, Integer> hash = new HashMap<>();
		try{
			Connection input = getConnection();
			PreparedStatement prepared_statement = input.prepareStatement(query);
			ResultSet result_set = prepared_statement.executeQuery();
                        ResultSetMetaData result_metadata = result_set.getMetaData();
                        int columnCount = result_metadata.getColumnCount();
			for (int i = 1; i <= columnCount; i++ ) {
    				hash.put(result_metadata.getColumnName(i),(Integer)i);
			}
			input.close();
		}
		catch(SQLException sqle){
			printSQLException(sqle);
		}
		return hash;
	}
}
