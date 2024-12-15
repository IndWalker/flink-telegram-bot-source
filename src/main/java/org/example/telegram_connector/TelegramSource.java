package org.example.telegram_connector;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.example.telegram_connector.api.TelegramApiHelper;
import org.example.telegram_connector.container.TelegramMessage;
import org.example.telegram_connector.enumerator.TelegramEnumeratorState;
import org.example.telegram_connector.enumerator.TelegramEnumeratorStateSerializer;
import org.example.telegram_connector.enumerator.TelegramSplitEnumerator;
import org.example.telegram_connector.split.TelegramSplit;
import org.example.telegram_connector.split.TelegramSplitSerializer;

public class TelegramSource implements Source<TelegramMessage, TelegramSplit, TelegramEnumeratorState> {

    public TelegramSource(String botToken) {
        TelegramApiHelper helper = TelegramApiHelper.getInstance();
        helper.setBotToken(botToken);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<TelegramMessage, TelegramSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new TelegramSourceReader(
                TelegramSplitReader::new,
                new TelegramRecordEmitter(),
                readerContext
        );
    }

    @Override
    public SplitEnumerator<TelegramSplit, TelegramEnumeratorState> createEnumerator(SplitEnumeratorContext<TelegramSplit> enumContext) {
        return new TelegramSplitEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<TelegramSplit, TelegramEnumeratorState> restoreEnumerator(SplitEnumeratorContext<TelegramSplit> enumContext, TelegramEnumeratorState checkpoint) {
        return new TelegramSplitEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<TelegramSplit> getSplitSerializer() {
        return new TelegramSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<TelegramEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new TelegramEnumeratorStateSerializer();
    }
}

