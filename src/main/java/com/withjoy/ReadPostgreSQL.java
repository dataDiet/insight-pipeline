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

	public static HashMap<String, Integer> getSQLHash(String query, String single_input){
		HashMap<String, Integer> hash = new HashMap<>();
		try{
			Connection input = getConnection();
			PreparedStatement prepared_statement = input.prepareStatement(query);
			prepared_statement.setString(1, single_input);
			ResultSet result_set = prepared_statement.executeQuery();

			while(result_set.next()){
				hash.put(result_set.getString(1), Integer.parseInt(result_set.getString(2)));
			}
			input.close();
		}
		catch(SQLException sqle){
			printSQLException(sqle);
		}
		return hash;
	}
}
