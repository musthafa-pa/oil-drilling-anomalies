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
        List<CSVRecord> recordList = new ArrayList<>();
        int attempts = 10;

        try (FileReader reader = new FileReader(csvFile)) {
            Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            for (CSVRecord record : csvRecords) {
                recordList.add(record);
                if (recordList.size() % 10 == 0 && attempts != 0) {
                    attempts--;

                    // Create new lists for each batch
                    List<Object> SPPA = new ArrayList<>();
                    List<Object> ROP30S = new ArrayList<>();
                    List<Object> TQ30S = new ArrayList<>();
                    List<Object> ECD_MW_IN = new ArrayList<>();

                    // Process each record in the current batch
                    for (CSVRecord rec : recordList) {
                        System.out.println(Arrays.toString(rec.values()));
                        SPPA.add(rec.get(10)); // Adjust index as needed
                        ROP30S.add(rec.get(11)); // Adjust index as needed
                        // Add other values similarly if needed
                        // TQ30S.add(rec.get(382)); // Adjust index as needed
                        // ECD_MW_IN.add(rec.get(457)); // Adjust index as needed
                    }

                    // Put the current batch data into the map
                    Map<String, List<Object>> streamData = new HashMap<>();
                    streamData.put("SPPA", SPPA);
                    streamData.put("ROP30S", ROP30S);
                    streamData.put("TQ30S", TQ30S);
                    streamData.put("ECD_MW_IN", ECD_MW_IN);

                    // Send the message
                    sendMessageToTopic(streamData.toString());

                    // Wait before processing the next batch
                    Thread.sleep(2000);

                    // Clear the recordList for the next batch
                    recordList.clear();
                }
            }

            // Process any remaining records if needed
            if (!recordList.isEmpty()) {
                List<Object> SPPA = new ArrayList<>();
                List<Object> ROP30S = new ArrayList<>();
                List<Object> TQ30S = new ArrayList<>();
                List<Object> ECD_MW_IN = new ArrayList<>();

                for (CSVRecord rec : recordList) {
                    System.out.println(Arrays.toString(rec.values()));
                    SPPA.add(rec.get(10)); // Adjust index as needed
                    ROP30S.add(rec.get(11)); // Adjust index as needed
                    // Add other values similarly if needed
                    // TQ30S.add(rec.get(382)); // Adjust index as needed
                    // ECD_MW_IN.add(rec.get(457)); // Adjust index as needed
                }

                Map<String, List<Object>> streamData = new HashMap<>();
                streamData.put("SPPA", SPPA);
                streamData.put("ROP30S", ROP30S);
                streamData.put("TQ30S", TQ30S);
                streamData.put("ECD_MW_IN", ECD_MW_IN);

                sendMessageToTopic(streamData.toString());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}