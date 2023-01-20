package it.unipi.dii.aide.mircv.common.utils.serializers;

import it.unipi.dii.aide.mircv.common.bean.DocumentIndexStats;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

public class CustomSerializerDocumentIndexStats extends GroupSerializerObjectArray<DocumentIndexStats> {
    @Override
    public void serialize(DataOutput2 dataOutput2, DocumentIndexStats documentIndexStats) throws IOException {
        dataOutput2.writeUTF(documentIndexStats.getDoc_no());
        dataOutput2.writeInt(documentIndexStats.getDoc_len());
    }

    @Override
    public DocumentIndexStats deserialize(DataInput2 dataInput2, int i) throws IOException {
        String doc_no = dataInput2.readUTF();
        int doc_len = dataInput2.readInt();

        return new DocumentIndexStats(doc_no, doc_len);
    }
}
