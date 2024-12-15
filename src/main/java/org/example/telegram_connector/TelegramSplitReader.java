package org.example.telegram_connector;

import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.example.telegram_connector.api.TelegramApiHelper;
import org.example.telegram_connector.container.TelegramIntermediateRecord;
import org.example.telegram_connector.split.TelegramSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TelegramSplitReader implements SplitReader<TelegramIntermediateRecord, TelegramSplit> {

    private TelegramSplit currentSplit;
    private final AtomicBoolean wakeUpFlag = new AtomicBoolean(false);
    private boolean finished = false;

    private final TelegramApiHelper helper = TelegramApiHelper.getInstance();

    public TelegramSplitReader() {}

    @Override
    public RecordsBySplits<TelegramIntermediateRecord> fetch() {
        wakeUpFlag.compareAndSet(true, false);
        RecordsBySplits.Builder<TelegramIntermediateRecord> builder = new RecordsBySplits.Builder<>();

        List<TelegramIntermediateRecord> updates = new ArrayList<>();
        updates.add(currentSplit.getRecord());

        if (wakeUpFlag.get()) {
            return builder.build();
        }

        builder.addAll(currentSplit.splitId(), updates);

        this.finished = true;
        builder.addFinishedSplit(currentSplit.splitId());
        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TelegramSplit> splitsChange) {
        for (TelegramSplit split : splitsChange.splits()) {
            this.currentSplit = split;
            this.finished = false;
        }
    }

    @Override
    public void wakeUp() {
        wakeUpFlag.compareAndSet(false, true);
    }

    @Override
    public void close() throws Exception {}
}

