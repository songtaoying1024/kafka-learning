package de.qimia;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String boostrapServer="127.0.0.1:9092";
        //String groupID="my-seven-application";
        String topic="first_topic";
        //create consumer configs
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //earliest means u want to read from the very beginning
        //latest means u want to read only the new messages

        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);
        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom=new TopicPartition(topic,0);
        long offsetToreadFrom=15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));


        //seek
        consumer.seek(partitionToReadFrom,offsetToreadFrom);
        int numberOfMessagesToRead=5;
        boolean keepOnReading=true;
        int numberOfMessagesReadSoFar=0;

        //poll for new data
        while (keepOnReading){
            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(100)); //
            for(ConsumerRecord<String,String> record:records){
                numberOfMessagesReadSoFar+=1;
                logger.info("Key: "+record.key()+", value: "+record.value());
                logger.info("Partition: "+record.partition()+ ", Offset: "+record.offset());
                if(numberOfMessagesReadSoFar>=numberOfMessagesToRead){
                    keepOnReading=false;
                    break;
                }
            }

        }
        logger.info("exit the application");
    }

}
