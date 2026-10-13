package ru.monyamau.task_tracker_email_sender.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.mail.MailSendException;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    private final String letterTopic;

    public KafkaConfig(@Value("${kafka.topic}") String letterTopic) {
        this.letterTopic = letterTopic;
    }

    @Bean
    public NewTopic createLetterTopic() {
        return TopicBuilder.name(letterTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(3000, 2));
        errorHandler.addRetryableExceptions(MailSendException.class);
        return errorHandler;
    }
}