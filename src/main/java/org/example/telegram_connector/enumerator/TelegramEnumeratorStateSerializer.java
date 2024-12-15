package org.example.telegram_connector.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TelegramEnumeratorStateSerializer implements SimpleVersionedSerializer<TelegramEnumeratorState> {

    private static final int VERSION = 1;

    private static final Logger LOG = LoggerFactory.getLogger(TelegramEnumeratorStateSerializer.class);

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TelegramEnumeratorState state) {
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            out.writeLong(state.getNextOffsetUpdateId());
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public TelegramEnumeratorState deserialize(int version, byte[] serialized) {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        try {
            long nextOffset = in.readLong();
            return new TelegramEnumeratorState(nextOffset);
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
            return null;
        }
    }
}

