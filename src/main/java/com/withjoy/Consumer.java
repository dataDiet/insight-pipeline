package com.withjoy;
/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.json.*;
import org.apache.log4j.*;
import org.apache.commons.csv.*;
import java.io.IOException;


/**
 *
 * @author akiramadono
 */
public class Consumer {
    
      private static boolean stop = false;
      private static HashMap<String, Integer> activity_string_int = new HashMap<>();
      private static HashMap<String, Integer> tracks_string_int = new HashMap<>();
      private static HashMap<Integer, String> activity_int_string = new HashMap<>();
      private static HashMap<Integer, String> tracks_int_string = new HashMap<>();
      private static String current_table_name = "";
      
      public static void main(String[] argv) throws Exception{
          
          if (argv.length != 2) {
              System.err.printf("Usage: %s <topicName> <groupId>\n",
                      Consumer.class.getSimpleName());
              System.exit(-1);
          }
          String topicName = argv[0];
          String groupId = argv[1];
          
          Logger logger = Logger.getLogger("org.apache.kafka.clients.consumer.ConsumerConfig");
          logger.setLevel(Level.FATAL);
          logger = Logger.getLogger("org.apache.kafka.common.utils.AppInfoParser");
          logger.setLevel(Level.FATAL);
          logger = Logger.getLogger("org.apache.kafka.clients.consumer.internals.AbstractCoordinator");
          logger.setLevel(Level.FATAL);
          logger = Logger.getLogger("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator");
          logger.setLevel(Level.FATAL);

  

          ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
          consumerRunnable.start();
          consumerRunnable.join();
          
      }

      private static class ConsumerThread extends Thread{
          private String topicName;
          private String groupId;
          
          private KafkaConsumer<String,String> kafkaConsumer;

          
          public ConsumerThread(String topicName, String groupId){
              this.topicName = topicName;
              this.groupId = groupId;
          }
          
          public void run() {
              Properties properties = new AcquireProperties("properties.txt").getProperties();
              Properties kafka_consumer_config_properties = new Properties();
              kafka_consumer_config_properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("host_port_pairs"));
              kafka_consumer_config_properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
              kafka_consumer_config_properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
              kafka_consumer_config_properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
              kafka_consumer_config_properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
              
              //Initialize Kafka Consumer
              kafkaConsumer = new KafkaConsumer<String, String>(kafka_consumer_config_properties);
              kafkaConsumer.subscribe(Arrays.asList(topicName));
              List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
              final int min_batch_size= Integer.parseInt(properties.getProperty("min_batch_size"));
              
              int record_number= 0;
              //Start processing messages
              try {
                  outerloop:
                  while (true) {
                          ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                          for (ConsumerRecord<String, String> record : records){
                              if(record!=null){
                                  System.out.println(csvStringProcessor(record.value()));
                              }
                              if(record_number > min_batch_size){
                                  kafkaConsumer.commitSync();
                                  break outerloop;
                              }
                              record_number++;
                          }
                  }
              }catch(WakeupException ex){
                  System.err.println("Exception caught " + ex.getMessage());
              }finally{
                  kafkaConsumer.close();
                  kafkaConsumer.wakeup();
              }
          }
          
          public KafkaConsumer<String,String> getKafkaConsumer(){
             return this.kafkaConsumer;
          }
      }
    public static void invertHashMap(HashMap<String,Integer> input_hash, HashMap<Integer, String> output_hash){
        Iterator<Map.Entry<String,Integer>> iterator = input_hash.entrySet().iterator();
        while(iterator.hasNext()){
            String table_name=iterator.next().getKey();
            output_hash.put(input_hash.get(table_name), table_name);
        }
    }
    
    public static void resetTableData(String table){

        Properties properties = new AcquireProperties("properties.txt").getProperties();
        String schema = properties.getProperty("pg_schema");

        activity_string_int.clear();
        activity_int_string.clear();
        tracks_string_int.clear();
        tracks_int_string.clear();
        activity_string_int = ReadPostgreSQL.getSQLHash("SELECT * FROM "+schema+"."+table+" LIMIT 1");
        tracks_string_int = ReadPostgreSQL.getSQLHash("SELECT * FROM "+schema+"."+"tracks"+" LIMIT 1");
        invertHashMap(activity_string_int,activity_int_string);
        invertHashMap(tracks_string_int,tracks_int_string);
    }
    
    public static String csvStringProcessor(String input){
       
        int pipe_position=input.indexOf('|');
        String table = input.substring(0,pipe_position);
        
        if(!current_table_name.equals(table)){
            current_table_name=table;
            resetTableData(table);
        }
        
        input=input.substring(pipe_position+1).replace("|", "");
        
        int tracks_num_columns=tracks_int_string.size();
        int activity_num_columns=activity_int_string.size();
        List<String> tsv_row=new ArrayList<>(tracks_num_columns);
        String output_left=null;
        JSONObject output_right =new JSONObject();

        try{
            CSVRecord csv_records = CSVParser.parse(input, CSVFormat.MYSQL).getRecords().get(0);                

            int i=1;
            for(;i <= tracks_num_columns;i++){
                String tracks_column= tracks_int_string.get((Integer)i);
                if(tracks_column.equals("event")){
                    tsv_row.add(table);
                }
                else{
                    if(activity_string_int.containsKey(tracks_column)){
                        String string_data=csv_records.
                            get(activity_string_int.
                                get(tracks_column)-1);
                        if(string_data == null){
                            tsv_row.add("");
                        }
                        else{
                            tsv_row.add(string_data);
                        }
                    }
                    else{
                        tsv_row.add("");
                    }
                }
            }
            output_left = String.join("|", tsv_row);

            for(i=1;i <= activity_num_columns;i++){
                    String activity_column = activity_int_string.get((Integer)i);
                    if(!tracks_string_int.containsKey(activity_column)){
                            String string_data_json=csv_records.
                                    get(activity_string_int.
                                                    get(activity_column)-1);
                            if(string_data_json == null){
                                    output_right.put(activity_column,"");
                            }
                            else{
                                    output_right.put(activity_column,string_data_json);  
                            }
                    }
            }
            return output_left+'|'+output_right.toString();
        }
        catch(IOException e){
                System.err.println("Problem reading record: "+input);
        }
        catch(JSONException e){
                System.err.println("Problem making json: "+input);
        }
        return "";
    }
}

