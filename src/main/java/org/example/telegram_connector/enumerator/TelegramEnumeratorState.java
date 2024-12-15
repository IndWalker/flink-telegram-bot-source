package org.example.telegram_connector.enumerator;

public class TelegramEnumeratorState {
    private final long nextOffsetUpdateId;

    public TelegramEnumeratorState(long nextOffsetUpdateId) {
        this.nextOffsetUpdateId = nextOffsetUpdateId;
    }

    public Long getNextOffsetUpdateId() {
        return nextOffsetUpdateId;
    }
}

