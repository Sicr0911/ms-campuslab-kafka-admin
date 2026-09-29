package com.campuslab.kafkaadmin.controller;

import com.campuslab.kafkaadmin.dto.ConsumerGroupInfoDTO;
import com.campuslab.kafkaadmin.dto.TopicInfoDTO;
import com.campuslab.kafkaadmin.service.KafkaAdminService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * API de solo lectura sobre el estado del cluster Kafka de CampusLab
 * (ec2-kafka), pensada para un panel operativo de Admin. No crea ni borra
 * topicos: es un microservicio de observabilidad.
 */
@RestController
@RequestMapping("/api/admin/kafka")
@RequiredArgsConstructor
@PreAuthorize("hasRole('ADMIN')")
public class KafkaAdminController {

    private final KafkaAdminService service;

    /** GET /api/admin/kafka/topics */
    @GetMapping("/topics")
    public ResponseEntity<List<TopicInfoDTO>> topics() {
        return ResponseEntity.ok(service.listarTopicos());
    }

    /** GET /api/admin/kafka/consumer-groups */
    @GetMapping("/consumer-groups")
    public ResponseEntity<List<ConsumerGroupInfoDTO>> consumerGroups() {
        return ResponseEntity.ok(service.listarConsumerGroups());
    }
}
