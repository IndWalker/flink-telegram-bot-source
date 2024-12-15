package org.example.telegram_connector.split;

import org.example.telegram_connector.container.TelegramIntermediateRecord;

public class TelegramSplitState {
    private TelegramIntermediateRecord record;

    public TelegramSplitState(TelegramIntermediateRecord record) {
        this.record = record;
    }

    public TelegramIntermediateRecord getRecord() {
        return this.record;
    }
}

