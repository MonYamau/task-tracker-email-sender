package ru.monyamau.task_tracker_email_sender.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.mail.MailAuthenticationException;
import org.springframework.mail.MailParseException;
import org.springframework.mail.MailPreparationException;
import org.springframework.util.backoff.FixedBackOff;
import ru.monyamau.task_tracker_email_sender.dto.LetterMessageDto;

import java.util.Map;

@Configuration
public class KafkaConfig {
    private static final long MS_INTERVAL = 5000L;
    private static final long NUM_OF_RETRIES = 2L;

    private final String mailingTopic;
    private final String dltMailingTopic;

    public KafkaConfig(@Value("${kafka.topics.mailing}") String mailingTopic,
                       @Value("${kafka.topics.dlt-mailing}") String dltMailingTopic) {
        this.mailingTopic = mailingTopic;
        this.dltMailingTopic = dltMailingTopic;
    }

    @Bean
    public NewTopic createMailingTopic() {
        return TopicBuilder.name(mailingTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic createDltMailingTopic() {
        return TopicBuilder.name(dltMailingTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public ProducerFactory<String, Object> dltProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        DelegatingByTypeSerializer typeSerializer = new DelegatingByTypeSerializer(
                Map.of(
                        byte[].class, new ByteArraySerializer(),
                        LetterMessageDto.class, new JacksonJsonSerializer<>()
                )
        );
        return new DefaultKafkaProducerFactory<>(producerProperties, new StringSerializer(), typeSerializer);
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(
            @Qualifier("dltProducerFactory") ProducerFactory<String, Object> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(
            @Qualifier("dltKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(dltMailingTopic, record.partition()));
        recoverer.setThrowIfNoDestinationReturned(true);
        recoverer.setLogRecoveryRecord(true);
        recoverer.setFailIfSendResultIsError(true);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(MS_INTERVAL, NUM_OF_RETRIES));
        errorHandler.addNotRetryableExceptions(MailAuthenticationException.class,
                MailParseException.class,
                MailPreparationException.class);
        return errorHandler;
    }
}