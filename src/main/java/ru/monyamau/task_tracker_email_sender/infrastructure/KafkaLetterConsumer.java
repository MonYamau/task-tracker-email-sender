package ru.monyamau.task_tracker_email_sender.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_email_sender.dto.LetterMessageDto;
import ru.monyamau.task_tracker_email_sender.service.EmailService;

@Slf4j
@Component
public class KafkaLetterConsumer {
    private final EmailService emailService;

    public KafkaLetterConsumer(EmailService emailService) {
        this.emailService = emailService;
    }

    @KafkaListener(topics = "${kafka.topic}")
    public void consume(LetterMessageDto messageDto) {
        log.info("Успешно получена задача на отправку письма для {}", messageDto.recipient());
        emailService.sendLetter(messageDto);
    }
}