package com.drilling_poc.controller;
import com.drilling_poc.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map; // Import the Map interface
import com.drilling_poc.service.WellLogService;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;
    @Autowired WellLogService wellLogService;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message){
        try{
            publisher.sendMessageToTopic(message);
            return ResponseEntity.ok("Message sent!");
        }catch (Exception exception){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish/json")
    public ResponseEntity<?> publishJSON(@RequestBody Map<String, Object> jsonData, @RequestBody String topic){
        try{
            publisher.sendJsonMessage(jsonData, topic);
            return ResponseEntity.ok("JSON Sent");
        }
        catch (Exception ex){
            System.out.println("Message" + ex.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish/well-log")
    public ResponseEntity<?> publishWellLog(){
        try{
            publisher.sendWellLogInInterval();
            return ResponseEntity.ok("Sending data to topic from csv is done...");
        }
        catch (Exception ex){
            System.out.println("Message" + ex.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/receive/well-log")
    public ResponseEntity<?> receiveWellLogs(@RequestBody Map<String, Object> requestBody) {
        try {
            // Strip any unwanted characters (like quotes) from the wellId
            // Call the service to fetch 10 records for the given well_id
            String id = (String) requestBody.get("well_id");
            List<Map<String, Object>> wellLogs = wellLogService.getWellLogs(id);

            // If no records are found, return an empty response
            if (wellLogs.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No data found for well_id: " + id);
            }

            // Return the list of well logs as the response
            return ResponseEntity.ok(wellLogs);
        } catch (Exception ex) {
            System.out.println("Error: " + ex.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}

