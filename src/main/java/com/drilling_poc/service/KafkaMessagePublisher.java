package com.drilling_poc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import java.io.FileReader;


@Service
public class KafkaMessagePublisher extends Thread{

    @Autowired
    private KafkaTemplate<String, Object> template;
    private final ObjectMapper objectMapper = new ObjectMapper();


    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> future = template.send("drilling-well-0001", message);
        future.whenComplete((result, exception)-> {
            if(exception == null){
                System.out.println("Send Message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]" );
            }
            else {
                System.out.println("Unable to send message=[" + message + "] due to: " +  exception.getMessage());
            }
        });
    }

    public void sendJsonMessage(Map<String, Object> jsonData) throws JsonProcessingException {
        String jsonString = objectMapper.writeValueAsString(jsonData);
        CompletableFuture<SendResult<String, Object>> future = template.send("drilling-well-0001", jsonString);
        future.whenComplete((result, exception)-> {
            if(exception == null){
                System.out.println("Send Message=[" + "she" + "] with offset=[" + result.getRecordMetadata().offset() + "]" );
            }
            else {
                System.out.println("Unable to send message=[" + "fge" + "] due to: " +  exception.getMessage());
            }
        });
    }

    public void sendWellLogInInterval() throws Exception {
        String csvFile = "/Users/musthafa/softway/DAI/spark-data-processing/csv_well_data/0001.csv";
        List<CSVRecord> recordList = new ArrayList<>();

        try(FileReader reader = new FileReader(csvFile)) {
            Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            for(CSVRecord record: csvRecords){
                recordList.add(record);

                if(recordList.size() % 10 == 0){
                    System.out.println("Rows:");
                    for(CSVRecord rec: recordList){
//                        System.out.println(rec);
                        Thread.sleep(300);
                        sendMessageToTopic(Arrays.toString(rec.values()));
                    }
                    recordList.clear();
                }
            }

            if(!recordList.isEmpty()){
                System.out.println("Remaining rows:");
                for(CSVRecord rec: recordList){
                    System.out.println(rec);
                }
            }
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

}