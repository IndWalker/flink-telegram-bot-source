package org.example.telegram_connector.container;

public class TelegramIntermediateRecord {
    private final long updateId;
    private final String chatId;
    private final String text;

    public TelegramIntermediateRecord(long updateId, String chatId, String text) {
        this.updateId = updateId;
        this.chatId = chatId;
        this.text = text;
    }

    public long getUpdateId() { return updateId; }
    public String getChatId() { return chatId; }
    public String getText() { return text; }
}

