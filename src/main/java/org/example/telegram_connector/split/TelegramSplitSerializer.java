package org.example.telegram_connector.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.example.telegram_connector.container.TelegramIntermediateRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelegramSplitSerializer implements SimpleVersionedSerializer<TelegramSplit> {

    private static final int VERSION = 1;

    private static final Logger LOG = LoggerFactory.getLogger(TelegramSplitSerializer.class);

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TelegramSplit split) {
        DataOutputSerializer out = new DataOutputSerializer(64);
        TelegramIntermediateRecord record = split.getRecord();
        try {
            out.writeLong(record.getUpdateId());
            out.writeUTF(record.getChatId());
            out.writeUTF(record.getText());
            out.writeUTF(split.splitId());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public TelegramSplit deserialize(int version, byte[] serialized) {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        try {
            long updateId = in.readLong();
            String chatId = in.readUTF();
            String text = in.readUTF();
            String splitId = in.readUTF();
            return new TelegramSplit(splitId, new TelegramIntermediateRecord(updateId, chatId, text));
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

