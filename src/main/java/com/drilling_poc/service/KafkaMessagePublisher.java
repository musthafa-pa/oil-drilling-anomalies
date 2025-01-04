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
import java.util.concurrent.*;


@Service
public class KafkaMessagePublisher extends Thread{

    @Autowired
    private KafkaTemplate<String, Object> template;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(2);


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

    public void sendJsonMessage(Map<String, Object> jsonData, String topic) throws JsonProcessingException {
        // Convert your JSON data to a string
        String jsonString = objectMapper.writeValueAsString(jsonData);

        // Send message to the specified topic dynamically
        CompletableFuture<SendResult<String, Object>> future = template.send(topic, jsonString);

        // Callback to handle the result
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                System.out.println("Sent Message=[" + jsonString + "] to topic=[" + topic + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + jsonString + "] due to: " + exception.getMessage());
            }
        });
    }

    public void sendWellLogInInterval() throws Exception {
        String csvFile = "/Users/musthafa/softway/DAI/spark-data-processing/well_data/well_drilling_log_001/00000001.csv";
        String csvFile2 = "/Users/musthafa/softway/DAI/spark-data-processing/well_data/well_drilling_log_002/00000001.csv";

        // Submit CSV file processing tasks to the executor
        executorService.submit(() -> processCsvFile(csvFile, "drilling-well-0001"));
        executorService.submit(() -> processCsvFile(csvFile2, "drilling-well-0002"));

        // Shut down the executor service once tasks are completed
        executorService.shutdown();
    }

    private void processCsvFile(String csvFile, String topic) {
        int attempts = 1000;

        try (FileReader reader = new FileReader(csvFile)) {
            // Parse CSV with the header
            Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            for (CSVRecord record : csvRecords) {
                if (attempts != 0) {
                    Map<String, Object> dummyData = new HashMap<>();
                    System.out.println(record);
                    attempts--;

                    // Accessing data using column names
                    dummyData.put("TIME", record.get("TIME"));
                    dummyData.put("SPPA", record.get("SPPA"));
                    dummyData.put("CPPA", record.get("CPPA"));
                    dummyData.put("ROP", record.get("ROP"));
                    System.out.println(dummyData);

                    // Send the message
                    sendJsonMessage(dummyData, topic);

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