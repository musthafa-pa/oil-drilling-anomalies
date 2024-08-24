package com.drilling_poc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.*;
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
        int attempts = 10;

        try (FileReader reader = new FileReader(csvFile)) {
            Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            for (CSVRecord record : csvRecords) {
                Map<String, Object> dummyData = new HashMap<String, Object>();
                if (attempts != 0) {
                    attempts--;
                    dummyData.put("TIME", record.get(0));
                    dummyData.put("SPPA", record.get(1));
                    dummyData.put("ROP30s", record.get(1));
                    dummyData.put("TQ30s", record.get(1));
                    dummyData.put("ECD_MW_IN", record.get(1));
                    System.out.println(dummyData);
                    // Send the message
                    sendJsonMessage(dummyData);
                    // Wait before processing the next batch
                    Thread.sleep(2000);

                    // Clear the recordList for the next batch
                    dummyData.clear();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}