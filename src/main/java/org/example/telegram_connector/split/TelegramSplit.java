package org.example.telegram_connector.split;

import org.example.telegram_connector.container.TelegramIntermediateRecord;

public class TelegramSplit implements org.apache.flink.api.connector.source.SourceSplit {
    private final String splitId;
    private final TelegramIntermediateRecord record;

    public TelegramSplit(String splitId, TelegramIntermediateRecord record) {
        this.splitId = splitId;
        this.record = record;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public TelegramIntermediateRecord getRecord() {
        return this.record;
    }
}

