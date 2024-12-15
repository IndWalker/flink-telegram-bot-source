package org.example.telegram_connector;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.example.telegram_connector.container.TelegramIntermediateRecord;
import org.example.telegram_connector.container.TelegramMessage;
import org.example.telegram_connector.split.TelegramSplit;
import org.example.telegram_connector.split.TelegramSplitState;

import java.util.Map;
import java.util.function.Supplier;

public class TelegramSourceReader extends SingleThreadMultiplexSourceReaderBase<
        TelegramIntermediateRecord,    // Intermediate type
        TelegramMessage,               // Output type
        TelegramSplit,                 // Split type
        TelegramSplitState> {          // Split state type

    private volatile boolean isRunning = true;

    public TelegramSourceReader(
            Supplier<SplitReader<TelegramIntermediateRecord, TelegramSplit>> splitReaderSupplier,
            RecordEmitter<TelegramIntermediateRecord, TelegramMessage, TelegramSplitState> recordEmitter,
            SourceReaderContext context) {
        super(splitReaderSupplier, recordEmitter, context.getConfiguration(), context);
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    protected void onSplitFinished(Map<String, TelegramSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected TelegramSplitState initializedState(TelegramSplit split) {
        return new TelegramSplitState(split.getRecord());
    }

    @Override
    protected TelegramSplit toSplitType(String splitId, TelegramSplitState splitState) {
        return new TelegramSplit(splitId, splitState.getRecord());
    }
}

