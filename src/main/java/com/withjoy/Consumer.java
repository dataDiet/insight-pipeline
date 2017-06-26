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
import java.util.Map.Entry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.*;
import org.apache.commons.csv.*;
import java.io.IOException;

/**
 *
 * @author akiramadono
 */
public class Consumer {

    private final static boolean stop = false;
    private static String current_table_name = "";
    private static CsvParser csv_parser;
    
    public static void main(String[] argv) throws Exception {

        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    Consumer.class.getSimpleName());
            System.exit(-1);
        }
        String topicName = argv[0];
        String groupId = argv[1];

        //disable logging by kafka in order not to push erroneous standard output which is
        //read by Redshift in the SSH Copy
        Logger logger = Logger.getLogger("org.apache.kafka.clients.consumer.ConsumerConfig");
        logger.setLevel(Level.FATAL);
        logger = Logger.getLogger("org.apache.kafka.common.utils.AppInfoParser");
        logger.setLevel(Level.FATAL);
        logger = Logger.getLogger("org.apache.kafka.clients.consumer.internals.AbstractCoordinator");
        logger.setLevel(Level.FATAL);
        logger = Logger.getLogger("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator");
        logger.setLevel(Level.FATAL);
        
        ConsumerThread consumerRunnable = new ConsumerThread(topicName, groupId);
        consumerRunnable.start();
        consumerRunnable.join();

    }

    private static class ConsumerThread extends Thread {

        private final String topicName;
        private final String groupId;

        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }
        
        @Override
        public void run() {
            Properties properties = new AcquireProperties("properties.txt").getProperties();
            Properties kafka_consumer_config_properties = new Properties();
            
            kafka_consumer_config_properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("host_port_pairs"));
            kafka_consumer_config_properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            kafka_consumer_config_properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            kafka_consumer_config_properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            kafka_consumer_config_properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            //Initialize Kafka Consumer
            kafkaConsumer = new KafkaConsumer<>(kafka_consumer_config_properties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
            final int min_batch_size = Integer.parseInt(properties.getProperty("min_batch_size"));

            int record_number = 0;
            //Start processing messages
            try {
                outerloop:
                while (true) {
                    
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        if (record != null) {
                            System.out.println(csvStringProcessor(record.value()));
                        }
                        if (record_number > min_batch_size) {
                            kafkaConsumer.commitSync();
                            break outerloop;
                        }
                        record_number++;
                    }
                }
            } catch (WakeupException ex) {
                System.err.println("Exception caught " + ex.getMessage());
            } finally {
                kafkaConsumer.close();
                kafkaConsumer.wakeup();
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }

    /**
     * Perform column mapping from metadata to csv input for appending tables
     *
     * @param input     CSV row from activity table
     * @return          uniformly formatted row for Redshift Database
     */
    public static String csvStringProcessor(String input) {
        int pipe_position = input.indexOf('|');
        String table = input.substring(0, pipe_position);
        input = input.substring(pipe_position + 1).replace("|", "");
        List<String> output_right = new ArrayList<>();
        List<String> tsv_row = new ArrayList<>();
        try{
            if (!current_table_name.equals(table)) {
                csv_parser = new CsvParser(table);
                csv_parser.getMainColumnMapping();
                current_table_name=table;
            }
            CSVRecord csv_records = CSVParser.parse(input, CSVFormat.MYSQL).getRecords().get(0);
            if(csv_parser != null){
                for(Entry<Integer,Integer> entry: csv_parser.getMain().entrySet()){
                    Integer array_mapping = entry.getValue();
                    
                    if(array_mapping.equals(-1)){
                        tsv_row.add("");
                    }
                    else{
                        String record=csv_records.get(array_mapping -1);
                        tsv_row.add((record == null) ? "": record);
                    }
                }
                for(Entry<String, Integer> entry: csv_parser.getExtra().entrySet()){
                    String record = csv_records.get(entry.getValue()-1);
                    String record_store = (record == null) ? "" : record;
                    output_right.add('"'+entry.getKey()+'"'+":" + '"'+record_store+'"');
                } 
            }
            return String.join("|", tsv_row) + '|' + '{'+String.join(",",output_right)+'}';
        }
        catch (IOException e) {
            System.err.println("Problem reading record: " + input);
        } 
        return "";
    }
}
