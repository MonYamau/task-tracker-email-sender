package ru.monyamau.task_tracker_email_sender.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import ru.monyamau.task_tracker_email_sender.dto.LetterMessageDto;

@Slf4j
@Service
public class EmailService {
    private final JavaMailSender mailSender;
    private final String from;

    public EmailService(JavaMailSender mailSender, @Value("${mail.sender}") String from) {
        this.mailSender = mailSender;
        this.from = from;
    }

    public void sendLetter(LetterMessageDto messageDto) {
        SimpleMailMessage simpleMailMessage = new SimpleMailMessage();
        simpleMailMessage.setFrom(from);
        simpleMailMessage.setTo(messageDto.recipient());
        simpleMailMessage.setSubject(messageDto.title());
        simpleMailMessage.setText(messageDto.text());
        try {
            mailSender.send(simpleMailMessage);
            log.info("Письмо направлено пользователю {}", messageDto.recipient());
        } catch (MailException e) {
            log.error("Возникла ошибка при попытке отправки письма пользователю {}", messageDto.recipient(), e);
            throw e;
        }
    }
}