package it.unipi.utils.serializers;

import it.unipi.bean.TermStats;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

public class CustomSerializerTermStats extends GroupSerializerObjectArray<TermStats> {
    @Override
    public void serialize(DataOutput2 dataOutput2, TermStats termStats) throws IOException {
        dataOutput2.writeInt(termStats.getDoc_frequency());
        dataOutput2.writeInt(termStats.getColl_frequency());
        dataOutput2.writeLong(termStats.getOffset_doc_id_start());
        dataOutput2.writeLong(termStats.getOffset_term_freq_start());
        dataOutput2.writeLong(termStats.getOffset_doc_id_end());
        dataOutput2.writeLong(termStats.getOffset_term_freq_end());
    }

    @Override
    public TermStats deserialize(DataInput2 dataInput2, int i) throws IOException {
        int doc_freq = dataInput2.readInt();
        int coll_freq = dataInput2.readInt();
        long doc_id_start = dataInput2.readLong();
        long term_freq_start = dataInput2.readLong();
        long doc_id_end = dataInput2.readLong();
        long term_freq_end = dataInput2.readLong();

        return new TermStats(doc_freq, coll_freq, doc_id_start, term_freq_start, doc_id_end, term_freq_end);
    }
}
