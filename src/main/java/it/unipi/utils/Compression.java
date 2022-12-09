package it.unipi.utils;

import it.unipi.builddatastructures.MergeBlocks;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class Compression {

    private static BitSet bitUnary;
    private static BitSet bitGamma;
    private static int totUnary;
    private static int totGamma;

    public Compression(){
        bitUnary=new BitSet();
        totUnary=0;
    }

    public static void gammaEncoding(int n) {
        String binN= Integer.toBinaryString(n);
        bitGamma.set(totGamma,totGamma+binN.length() - 1);
        bitGamma.clear(totGamma+binN.length()-1);
        for (int i = 1; i < binN.length(); i++) {
            if(binN.charAt(i)=='1')
                bitGamma.set(totGamma+binN.length()-1+i);
            else
                bitGamma.clear(totGamma+binN.length()-1+i);
        }
        totGamma+=(2*binN.length()-1);

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

    public static void unaryEncoding(int n) {
        bitUnary.set(totUnary,totUnary+n - 1, true);
        bitUnary.clear(totUnary+n-1);
        totUnary+=n;
    }

    public static BitSet getUnaryBitSet(){
        BitSet bitSet=new BitSet(totUnary);
        bitSet=(BitSet) bitUnary.clone();
        return bitSet;
    }

    public static BitSet getGammaBitSet(){
        BitSet bitSet=new BitSet(totGamma);
        bitSet=(BitSet) bitGamma.clone();
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