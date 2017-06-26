/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.withjoy;

import java.util.*;

/**
 *
 * @author akiramadono
 */
public class CsvParser {
    private final HashMap<String, Integer> reference_string_int;    
    private final TreeMap<String, Integer> specific_table_string_int;
    private final TreeMap<Integer, Integer> reference_colnum_activity_colnum;
    /**
     * reference table is assumed to be named in pg_reference_table parameter
     * @param specific_table_name table name of the table to append
     */
    public CsvParser(String specific_table_name){
        ReadPostgreSQL pg_methods = new ReadPostgreSQL();
        Properties properties = new AcquireProperties("properties.txt").getProperties();
        String reference_table = properties.getProperty("pg_reference_table");
        reference_string_int = pg_methods.GetTableData(reference_table);
        specific_table_string_int = new TreeMap(pg_methods.GetTableData(specific_table_name));
        reference_colnum_activity_colnum = new TreeMap<>();
    }
    
    /**
     * Map column names and strings to ordered TreeMaps for reference table and new table.
     */
    public void getMainColumnMapping(){
        for(String colname : reference_string_int.keySet()){
            if(specific_table_string_int.containsKey(colname)){
                reference_colnum_activity_colnum.put(reference_string_int.get(colname), specific_table_string_int.get(colname));
                specific_table_string_int.remove(colname);
            }
            else{
                // -1 for columns in reference table that aren't in activity table
                reference_colnum_activity_colnum.put(reference_string_int.get(colname),-1);
            }
        }
    }
    
    public TreeMap<Integer, Integer> getMain(){
        return reference_colnum_activity_colnum;
    }
    
    public TreeMap<String, Integer> getExtra(){ 
       return specific_table_string_int;
    }
}
