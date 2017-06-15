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

import java.util.*;
import java.sql.*;
import java.util.Map.Entry;


public class BatchFlinkJob {
        
                
        public static void main(String[] args){
            try{
                HashMap<String, Integer> activity_columns = ReadPostgreSQL.getSQLHash("SELECT attname, attnum FROM pg_attribute a, pg_class b WHERE a.attrelid=b.oid and b.relname = ? and a.attnum > 0 GROUP BY attname, attnum;", 
                            "signed_out");
                HashMap<String, Integer> tracks_columns = ReadPostgreSQL.getSQLHash("SELECT attname, attnum FROM pg_attribute a, pg_class b WHERE a.attrelid=b.oid and b.relname = ? and a.attnum > 0 GROUP BY attname, attnum;", 
                            "tracks");
                
                Iterator<Entry<String, Integer>> activity_iterator = activity_columns.entrySet().iterator();
                ArrayList<Entry<String, Integer>> json_vars = new ArrayList<>();
                ArrayList<Entry<String, Integer>> keep_vars = new ArrayList<>();
                
                while(activity_iterator.hasNext()){
                    Entry<String, Integer> consider=activity_iterator.next();
                    if(tracks_columns.containsKey(consider.getKey())){
                        keep_vars.add(consider);
                        System.out.println("keep "+consider.getKey());
                    }
                    else{
                        json_vars.add(consider);
                        System.out.println("json "+consider.getKey());
                    }
                }         
            }
            catch(SQLException e){
                ReadPostgreSQL.printSQLException(e);
            }
        }
        public static void notMain(String[] args) throws Exception {
            // set up the batch execution environment
            final ParameterTool params = ParameterTool.fromArgs(args);
            
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
           
            DataSet<String> text = env.readTextFile(params.get("input"));
            DataSet<String> counts = text.map(new Parser());
            
            if (params.has("output")) {
                counts.print();
                counts.writeAsText(params.get("output"),WriteMode.OVERWRITE);
                // execute program
                env.execute("Wrote text file output");
            } else {
                System.out.println("Printing result to stdout. Use --output to specify output path.");
                counts.print();
            }
 
	}

    // User-defined functions
    public static final class Parser implements MapFunction<String, String> {
        @Override
        public String map(String input) {
            // normalize and split the line
            return input.length()+input;
        }

    }
}
