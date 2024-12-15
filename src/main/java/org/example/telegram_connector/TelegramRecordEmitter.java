package org.example.telegram_connector;

import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.api.connector.source.SourceOutput;
import org.example.telegram_connector.container.TelegramIntermediateRecord;
import org.example.telegram_connector.container.TelegramMessage;
import org.example.telegram_connector.split.TelegramSplitState;

public class TelegramRecordEmitter implements RecordEmitter<TelegramIntermediateRecord, TelegramMessage, TelegramSplitState> {

    @Override
    public void emitRecord(TelegramIntermediateRecord element, SourceOutput<TelegramMessage> output, TelegramSplitState splitState) throws Exception {
        TelegramMessage msg = new TelegramMessage(element.getUpdateId(), element.getChatId(), element.getText());
        output.collect(msg);
    }
}

