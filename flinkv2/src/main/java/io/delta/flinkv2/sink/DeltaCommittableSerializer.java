package io.delta.flinkv2.sink;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.actions.SingleAction;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class DeltaCommittableSerializer implements SimpleVersionedSerializer<DeltaCommittable> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DeltaCommittable obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeUTF(obj.getAppId());
            out.writeUTF(obj.getWriterId());
            out.writeLong(obj.getCheckpointId());

            final byte[] rowBytes = JsonUtils.rowToJson(obj.getKernelActionRow()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(rowBytes.length);
            out.write(rowBytes);
            out.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public DeltaCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            final String appId = in.readUTF();
            final String writerId = in.readUTF();
            final long checkpointId = in.readLong();
            final int rowBytesLength = in.readInt();

            final byte[] rowBytes = new byte[rowBytesLength];
            in.readFully(rowBytes);
            final String rowJsonStr = new String(rowBytes, java.nio.charset.StandardCharsets.UTF_8);
            final Row kernelActionRow = JsonUtils.rowFromJson(rowJsonStr, SingleAction.FULL_SCHEMA);;

            return new DeltaCommittable(appId, writerId, checkpointId, kernelActionRow);
        }
    }
}
