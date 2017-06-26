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
import org.apache.kafka.clients.producer.*;
import java.io.BufferedReader;
import java.io.IOException;

/**
 *
 * @author akiramadono
 */
public class Producer {

    //private static Scanner in;
    private static S3Reader s3_in;
    final private static List<String> s3_file_list = new ArrayList();

    public static void main(String[] argv) {
        if (argv.length != 1) {
            System.err.println("Please specify the topic");
            System.exit(-1);
        }
        String topicName = argv[0];

        Properties properties = new AcquireProperties("properties.txt").getProperties();
        BufferedReader s3_reader = null;

        s3_in = new S3Reader(properties.getProperty("aws_id"), 
                                properties.getProperty("aws_secret"),
                                properties.getProperty("aws_region"));
        s3_in.getListObjectKeys(properties.getProperty("aws_bucket"), s3_file_list);

        //setup configuration for producer including cluster node ip host:port pairs
        Properties kafka_producer_config_properties = new Properties();
        kafka_producer_config_properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("host_port_pairs")); 
        kafka_producer_config_properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafka_producer_config_properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafka_producer_config_properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 300000);
        
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(kafka_producer_config_properties);
        try {
            // read in files from s3 and send to the Consumer
            for (String table : s3_file_list) {
                s3_reader = s3_in.readFromS3(properties.getProperty("aws_bucket"), table);
                String line;
                while ((line = s3_reader.readLine()) != null) {
                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, table + "|" + line);
                    producer.send(rec);
                }
            }
            producer.close();
        } catch (IOException e) {
            System.err.print("Couldn't read from S3");
        }
    }

}
