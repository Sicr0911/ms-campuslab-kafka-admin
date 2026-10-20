package com.campuslab.kafkaadmin.service;

import com.campuslab.kafkaadmin.dto.TopicInfoDTO;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaAdminServiceTest {

    private AdminClient adminClient;
    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        adminClient = mock(AdminClient.class);
        service = new KafkaAdminService(adminClient);
    }

    @Test
    void listarTopicos_calculaParticionesYFactorDeReplicacion() {
        Node b1 = new Node(1, "kafka-1", 9092);
        Node b2 = new Node(2, "kafka-2", 9092);
        TopicListing listing = new TopicListing("bookings.events", Uuid.randomUuid(), false);
        TopicDescription descripcion = new TopicDescription("bookings.events", false, List.of(
                new TopicPartitionInfo(0, b1, List.of(b1, b2), List.of(b1, b2)),
                new TopicPartitionInfo(1, b2, List.of(b2, b1), List.of(b2, b1)),
                new TopicPartitionInfo(2, b1, List.of(b1, b2), List.of(b1))));

        ListTopicsResult listTopics = mock(ListTopicsResult.class);
        when(listTopics.listings()).thenReturn(KafkaFuture.<Collection<TopicListing>>completedFuture(List.of(listing)));
        when(adminClient.listTopics()).thenReturn(listTopics);

        DescribeTopicsResult describeTopics = mock(DescribeTopicsResult.class);
        when(describeTopics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("bookings.events", descripcion)));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopics);

        List<TopicInfoDTO> topicos = service.listarTopicos();

        assertThat(topicos).containsExactly(new TopicInfoDTO("bookings.events", 3, 2, false));
    }

    @Test
    void kafkaNoResponde_lanzaKafkaAdminException() {
        KafkaFutureImpl<Collection<TopicListing>> fallido = new KafkaFutureImpl<>();
        fallido.completeExceptionally(new TimeoutException("broker no disponible"));

        ListTopicsResult listTopics = mock(ListTopicsResult.class);
        when(listTopics.listings()).thenReturn(fallido);
        when(adminClient.listTopics()).thenReturn(listTopics);

        assertThatThrownBy(() -> service.listarTopicos())
                .isInstanceOf(KafkaAdminException.class)
                .hasMessageContaining("topicos");
    }
}
