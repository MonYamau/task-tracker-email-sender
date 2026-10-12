package ru.monyamau.task_tracker_email_sender.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_email_sender.dto.LetterMessageDto;

@Slf4j
@Component
public class KafkaLetterConsumer {
    @KafkaListener(topics = "${kafka.topic}")
    public void consume(LetterMessageDto messageDto) {
        log.info("Successfully get letter for: {}", messageDto.recipient());
    }
}