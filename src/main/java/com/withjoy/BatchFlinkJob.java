package com.withjoy;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.commons.csv.*;
import java.util.*;
import java.util.Map.Entry;
import java.io.IOException;
import org.json.*;



public class BatchFlinkJob {
        
    private static HashMap<String, Integer> activity_columns_string_int;
    private static HashMap<String, Integer> tracks_columns_string_int;
    private static HashMap<Integer, String> activity_columns_int_string;
    private static HashMap<Integer, String> tracks_columns_int_string;

    public static void main(String[] args){
        //Determine relevant columns and their orderings for use in map
        activity_columns_string_int = ReadPostgreSQL.getSQLHash("SELECT attname, attnum "
                +"FROM pg_attribute a, pg_class b "
                +"WHERE a.attrelid=b.oid and b.relname = ? and a.attnum > 0"
                +"GROUP BY attname, attnum"
                +"ORDER BY attnum",
        
                        "signed_out");
        tracks_columns_string_int = ReadPostgreSQL.getSQLHash("SELECT attname, attnum "
                +"FROM pg_attribute a, pg_class b "
                +"WHERE a.attrelid=b.oid and b.relname = ? and a.attnum > 0 "
                +"GROUP BY attname, attnum" 
                +"ORDER BY attnum",
                        "tracks");
        invertHashMap(activity_columns_string_int,activity_columns_int_string);
        invertHashMap(tracks_columns_string_int,tracks_columns_int_string);
    }
    
    public static void otherMain(String[] args){
        // set up the batch execution environment
        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<String> text_with_json = text.map(new Parser());
        try{
        if (params.has("output")) {
            String aws_bucket = new AcquireProperties("properties.txt").getProperties().getProperty("aws_bucket");
            text_with_json.writeAsText("s3a://"+aws_bucket+"/"+params.get("output"),WriteMode.OVERWRITE);
            // execute program
            env.execute("Wrote text file output");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text_with_json.print();
        }
        }
        catch(Exception e){
            System.out.println("Flink Map Exception");
        }
    }
    
    public static void invertHashMap(HashMap<String,Integer> input_hash, HashMap<Integer, String> output_hash){
        Iterator<Entry<String,Integer>> iterator = input_hash.entrySet().iterator();
        while(iterator.hasNext()){
            String table_name=iterator.next().getKey();
            output_hash.put(input_hash.get(table_name), table_name);
        }
    }
    // User-defined functions
    public static final class Parser implements MapFunction<String, String> {
        @Override
        public String map(String tsv_input) {
            int tracks_num_columns=tracks_columns_int_string.size();
            int activity_num_columns=activity_columns_int_string.size();

            List<String> tsv_row=new ArrayList<>(tracks_num_columns);
            String output_left = null;
            JSONObject output_right = null;
            
            try{
                List<CSVRecord> tsv_records = CSVParser.parse(tsv_input, CSVFormat.TDF).getRecords();                
                int i=1;
                for(;i <= tracks_num_columns;i++){
                    if(activity_columns_string_int.containsKey(tracks_columns_int_string.get((Integer)i))){
                        tsv_row.add(i-1,
                                tsv_records.get(
                                activity_columns_string_int.
                                        get(tracks_columns_int_string.
                                                get((Integer)i))-1).toString());
                    }
                    else{
                        tsv_row.add(i, "NULL");
                    }
                }
                output_left = String.join("\t", tsv_row);
                for(i=1;i <= activity_num_columns;i++){
                    if(!tracks_columns_string_int.containsKey(activity_columns_int_string.get((Integer)i))){
                        output_right.put(activity_columns_int_string.get((Integer)i),
                            tsv_records.
                                get(activity_columns_string_int.
                                        get(activity_columns_int_string.
                                                get((Integer)i))-1).toString());
                                              
                    }
                }
                return output_left+output_right.toString();
            }
            catch(IOException e){
                System.err.println("Problem reading record: "+tsv_input);
            }
            catch(JSONException e){
                System.err.println("Problem making json: "+tsv_input);
            }
            return "";
        }
    }
}
