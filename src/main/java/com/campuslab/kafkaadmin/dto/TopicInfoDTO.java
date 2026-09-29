package com.campuslab.kafkaadmin.dto;

public record TopicInfoDTO(
        String name,
        int partitionCount,
        int replicationFactor,
        boolean internal
) {
}
