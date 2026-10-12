package ru.monyamau.task_tracker_email_sender.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    private final String letterTopic;

    public KafkaTopicConfig(@Value("${kafka.topic}") String letterTopic) {
        this.letterTopic = letterTopic;
    }

    @Bean
    public NewTopic createLetterTopic() {
        return TopicBuilder.name(letterTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
