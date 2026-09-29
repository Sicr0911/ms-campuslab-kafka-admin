package com.campuslab.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class KafkaAdminClientConfig {

    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000
        );
        return AdminClient.create(config);
    }
}
