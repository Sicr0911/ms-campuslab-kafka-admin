package com.campuslab.kafkaadmin.dto;

public record ConsumerGroupInfoDTO(
        String groupId,
        String state,
        int memberCount
) {
}
