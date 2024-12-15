package org.example.telegram_connector.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import org.example.telegram_connector.api.TelegramApiHelper;
import org.example.telegram_connector.container.TelegramIntermediateRecord;
import org.example.telegram_connector.split.TelegramSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelegramSplitEnumerator implements SplitEnumerator<TelegramSplit, TelegramEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(TelegramSplitEnumerator.class);

    private final SplitEnumeratorContext<TelegramSplit> context;
    private final int retryIntervalMs = 500;

    private TelegramEnumeratorState state;

    private final TelegramApiHelper apiHelper = TelegramApiHelper.getInstance();

    public TelegramSplitEnumerator(SplitEnumeratorContext<TelegramSplit> context, TelegramEnumeratorState checkpointState) {
        this.context = context;

        this.state = Objects.requireNonNullElseGet(checkpointState, this::initializeOffsetState);
    }

    @Override
    public void start() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        TelegramIntermediateRecord splitRecord = null;
        long nextOffsetUpdateId = this.state.getNextOffsetUpdateId();
        while (splitRecord == null) {
            splitRecord = pollTelegramApi(nextOffsetUpdateId);
        }

        TelegramSplit split = new TelegramSplit("split-" + nextOffsetUpdateId, splitRecord);
        context.assignSplits(new SplitsAssignment<>(split, subtaskId));
        this.state = new TelegramEnumeratorState(nextOffsetUpdateId + 1);
    }

    @Override
    public void addSplitsBack(List<TelegramSplit> splits, int subtaskId) {
        for (TelegramSplit split : splits) {
            context.assignSplits(new SplitsAssignment<>(split, subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public TelegramEnumeratorState snapshotState(long checkpointId) throws Exception {
        return state;
    }

    @Override
    public void close() throws IOException {

    }

    private TelegramIntermediateRecord pollTelegramApi(long offset) {
        try {
            Thread.sleep(retryIntervalMs);
            return apiHelper.getUpdates(offset);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return null;
        }
    }

    private TelegramEnumeratorState initializeOffsetState() {
        long offset = -1;
        while (offset < 0) {
            offset = initializeOffset();
            try {
                Thread.sleep(retryIntervalMs);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
        }

        return new TelegramEnumeratorState(offset);
    }

    private long initializeOffset() {
        try {
            JsonNode result = apiHelper.getUpdates();

            if (!result.isArray() || result.isEmpty()) {
                return -1;
            }

            return result.get(0).path("update_id").asLong();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return -1;
        }
    }
}

