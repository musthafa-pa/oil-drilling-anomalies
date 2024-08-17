package com.drilling_poc.controller;
import com.drilling_poc.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map; // Import the Map interface

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

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
    public ResponseEntity<?> publishJSON(@RequestBody Map<String, Object> jsonData){
        try{
            publisher.sendJsonMessage(jsonData);
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
            return ResponseEntity.ok("Sending data to topic from csv is in progress...");
        }
        catch (Exception ex){
            System.out.println("Message" + ex.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}

