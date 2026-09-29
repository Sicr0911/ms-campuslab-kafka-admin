package com.campuslab.kafkaadmin.service;

import com.campuslab.kafkaadmin.dto.ConsumerGroupInfoDTO;
import com.campuslab.kafkaadmin.dto.TopicInfoDTO;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Envuelve el AdminClient de Kafka en operaciones de solo lectura: no crea,
 * borra ni modifica topicos. Es un microservicio de observabilidad del
 * cluster (ec2-kafka), no de administracion destructiva.
 */
@Service
@RequiredArgsConstructor
public class KafkaAdminService {

    private static final int TIMEOUT_SEGUNDOS = 10;

    private final AdminClient adminClient;

    public List<TopicInfoDTO> listarTopicos() {
        try {
            List<TopicListing> listados = List.copyOf(
                    adminClient.listTopics().listings().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS));

            Map<String, TopicDescription> descripciones = adminClient
                    .describeTopics(listados.stream().map(TopicListing::name).toList())
                    .allTopicNames().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);

            return listados.stream()
                    .map(listing -> {
                        TopicDescription desc = descripciones.get(listing.name());
                        int particiones = desc.partitions().size();
                        int replicas = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
                        return new TopicInfoDTO(listing.name(), particiones, replicas, listing.isInternal());
                    })
                    .toList();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException("Consulta a Kafka interrumpida", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new KafkaAdminException("No fue posible listar los topicos de Kafka", ex);
        }
    }

    public List<ConsumerGroupInfoDTO> listarConsumerGroups() {
        try {
            List<ConsumerGroupListing> listados = List.copyOf(
                    adminClient.listConsumerGroups().valid().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS));

            Map<String, ConsumerGroupDescription> descripciones = adminClient
                    .describeConsumerGroups(listados.stream().map(ConsumerGroupListing::groupId).toList())
                    .all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);

            return listados.stream()
                    .map(listing -> {
                        ConsumerGroupDescription desc = descripciones.get(listing.groupId());
                        return new ConsumerGroupInfoDTO(
                                listing.groupId(),
                                desc.state().toString(),
                                desc.members().size());
                    })
                    .toList();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException("Consulta a Kafka interrumpida", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new KafkaAdminException("No fue posible listar los consumer groups de Kafka", ex);
        }
    }
}
