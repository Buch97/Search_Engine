package it.unipi.utils;

import it.unipi.builddatastructures.MergeBlocks;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class Compression {

    public static BitSet gammaEncoding(int n) {
        BitSet binN = intToBitSet(n);
        BitSet b = unaryEncoding(binN.size());
        BitSet bitSet = new BitSet(2 * binN.size() - 1);
        for (int i = 0; i < bitSet.size(); i++) {
            if (i < binN.size()) {
                bitSet.set(i, b.get(i));
            } else {
                bitSet.set(i, binN.get(b.size() - 2 + i - 3));
            }
        }
        return bitSet;
    }

    public static int gammaDecodingList(BitSet bitSet,int pos) {
        int i = pos;
        BitSet bs;

        while (i < bitSet.size()) {
            if (!bitSet.get(i))
                break;
            i++;
        }
        bs = new BitSet(i);
        bs.set(0, true);
        int cont = 1;
        while (cont < bs.size()) {
            i++;
            bs.set(cont, bitSet.get(i));
            cont++;
        }
        AuxObject.setPosG(i);
        return toInt(bs);
    }

    public static BitSet unaryEncoding(int n) {
        BitSet bitSet = new BitSet(n);
        if (n == 1) {
            bitSet.clear(0);
            return bitSet;
        }
        bitSet.set(0, n - 1, true);
        bitSet.clear(n - 1);
        return bitSet;
    }

    public static BitSet intToBitSet(int value) {
        BitSet bits = new BitSet();
        int index = 0;
        while (value != 0) {
            if (value % 2 != 0) {
                bits.set(index);
            }
            ++index;
            value = value >>> 1;
        }

        return bits;

    }

    public static int toInt(BitSet bitSet) {
        int intValue = 0;
        for (int bit = 0; bit < bitSet.length(); bit++) {
            if (bitSet.get(bit)) {
                intValue |= (1 << bit);
            }
        }
        return intValue;
    }



    public static int decodingUnaryList(BitSet bitSet, int pos) {
        int count=0;
        for (int i = pos; i < bitSet.size(); i++) {
            if (!bitSet.get(i)) {
                AuxObject.setPosU(++i);
                return ++count;
            } else {
                count++;
            }
        }
        return 0;
    }

    public static int unaryDecoding(BitSet bitSet) {
        return bitSet.cardinality() + 1;
    }

    public List<BitSet> listEncodingUnary(List<Integer> n) {
        List<BitSet> bs = new ArrayList<BitSet>(n.size());
        bs.add(unaryEncoding(n.get(0)));

        for (int i = 1; i < n.size(); i++)
            bs.add(unaryEncoding(n.get(i) - n.get(i - 1)));

        return bs;
    }
}