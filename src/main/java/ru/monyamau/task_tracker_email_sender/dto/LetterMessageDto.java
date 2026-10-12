package ru.monyamau.task_tracker_email_sender.dto;

public record LetterMessageDto(String recipient, String title, String text) {
}