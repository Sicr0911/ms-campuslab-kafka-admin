package com.campuslab.kafkaadmin;

import com.campuslab.kafkaadmin.dto.TopicInfoDTO;
import com.campuslab.kafkaadmin.service.KafkaAdminException;
import com.campuslab.kafkaadmin.service.KafkaAdminService;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Carga el contexto completo sin un cluster Kafka real (AdminClient
 * simulado) y verifica la seguridad y el manejo de errores de la API.
 */
@SpringBootTest
@AutoConfigureMockMvc
class MsCampuslabKafkaAdminApplicationTests {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AdminClient adminClient;

    @MockBean
    private KafkaAdminService service;

    @Test
    void contextLoads_yAdminListaTopicos() throws Exception {
        when(service.listarTopicos()).thenReturn(List.of(new TopicInfoDTO("bookings.events", 3, 1, false)));

        mockMvc.perform(get("/api/admin/kafka/topics")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("bookings.events"))
                .andExpect(jsonPath("$[0].partitionCount").value(3));
    }

    @Test
    void sinToken_responde401() throws Exception {
        mockMvc.perform(get("/api/admin/kafka/topics"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void sinRolAdmin_responde403() throws Exception {
        mockMvc.perform(get("/api/admin/kafka/topics")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_AUDITOR"))))
                .andExpect(status().isForbidden());
    }

    @Test
    void kafkaNoDisponible_responde502() throws Exception {
        when(service.listarConsumerGroups()).thenThrow(new KafkaAdminException("Kafka caido", null));

        mockMvc.perform(get("/api/admin/kafka/consumer-groups")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"))))
                .andExpect(status().isBadGateway());
    }
}
