package it.unipi.utils;

import it.unipi.builddatastructures.MergeBlocks;

import java.util.Arrays;
import java.util.BitSet;

public class Compression {

    private BitSet bitUnary;
    private BitSet bitGamma;
    private int posUnary;
    private int formerElem = 0;
    private int posGamma;

    public Compression() {
        bitUnary = new BitSet();
        bitGamma = new BitSet();
        posUnary = 0;
        posGamma = 0;
    }

    public void gammaEncoding(int n) {
        int gap = n - formerElem;
        formerElem = n;

        String binN = Integer.toBinaryString(gap);
        bitGamma.set(posGamma, posGamma + binN.length() - 1);
        bitGamma.clear(posGamma + binN.length() - 1);
        for (int i = 1; i < binN.length(); i++) {
            if (binN.charAt(i) == '1')
                bitGamma.set(posGamma + binN.length() - 1 + i);
            else
                bitGamma.clear(posGamma + binN.length() - 1 + i);
        }
        posGamma += (2 * binN.length() - 1);

    }

    public int gammaDecodingList(BitSet bitSet) {
        int i = bitSet.nextClearBit(posGamma);
        int sizebs = i + 1 - posGamma;

        BitSet bs = bitSet.get(i + 1, i + sizebs);
        posGamma = i + sizebs;

        int gap = (int) (Math.pow(2, sizebs) + convert(bs));
        int n = gap + formerElem;
        formerElem = n;
        return n;
    }

    public void unaryEncoding(int n) {
        bitUnary.set(posUnary, posUnary + n - 1);
        bitUnary.clear(posUnary + n - 1);
        posUnary += n;
    }

    public BitSet getUnaryBitSet() {
        return bitUnary;
    }

    public BitSet getGammaBitSet() {
        return bitGamma;
    }

    public int decodingUnaryList(BitSet bitSet) {
        int count = bitSet.nextClearBit(posUnary) + 1 - posUnary;
        posUnary = posUnary + count;
        return count;
    }

    public int getPosUnary() {
        return posUnary;
    }

    public int getPosGamma() {
        return posGamma;
    }


    public static int convert(BitSet bits) {
        int value = 0;
        for (int i = 0; i < bits.length(); ++i) {
            value += bits.get(i) ? (1 << i) : 0;
        }
        return value;
    }
}