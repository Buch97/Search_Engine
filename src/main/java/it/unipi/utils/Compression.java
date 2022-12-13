package it.unipi.utils;

import it.unipi.builddatastructures.MergeBlocks;

import java.util.Arrays;
import java.util.BitSet;

public class Compression {

    private BitSet bitUnary;
    private BitSet bitGamma;
    private int posUnary;
    private int posGamma;

    public Compression() {
        bitUnary = new BitSet();
        bitGamma = new BitSet();
        posUnary = 0;
        posGamma = 0;
    }

    public void gammaEncoding(int n) {
        String binN = Integer.toBinaryString(n);
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

    public int gammaDecodingList(BitSet bitSet, int size) {
        int i = posGamma;
        BitSet bs;

        while (i < size) {
            if (!bitSet.get(i))
                break;
            i++;
        }

        bs = new BitSet(i + 1);
        int sizebs = i + 1;

        bs.set(0, true);
        int cont = 1;
        while (cont < sizebs) {
            i++;
            bs.set(cont, bitSet.get(i));
            cont++;
        }
        posGamma = i + 1;
        return Integer.parseInt(BitSetToString(bs, sizebs), 2);
    }

    public void unaryEncoding(int n) {
        bitUnary.set(posUnary, posUnary + n - 1);
        bitUnary.clear(posUnary + n - 1);
        posUnary += n;
    }

    public BitSet getUnaryBitSet() {
        BitSet bitSet;
        bitSet = (BitSet) bitUnary.clone();
        return bitSet;
    }

    public BitSet getGammaBitSet() {
        BitSet bitSet = new BitSet(posGamma);
        bitSet = (BitSet) bitGamma.clone();
        return bitSet;
    }

    public int decodingUnaryList(BitSet bitSet, int size) {
        int count = 0;
        for (int i = posUnary; i < size; i++) {
            if (!bitSet.get(i)) {
                posUnary = ++i;
                return ++count;
            } else {
                count++;
            }
        }
        return 0;
    }

    public BitSet getBitUnary() {
        return bitUnary;
    }

    public void setBitUnary(BitSet bitUnary) {
        this.bitUnary = bitUnary;
    }

    public BitSet getBitGamma() {
        return bitGamma;
    }

    public void setBitGamma(BitSet bitGamma) {
        this.bitGamma = bitGamma;
    }

    public int getPosUnary() {
        return posUnary;
    }

    public void setPosUnary(int posUnary) {
        this.posUnary = posUnary;
    }

    public int getPosGamma() {
        return posGamma;
    }

    public void setPosGamma(int posGamma) {
        this.posGamma = posGamma;
    }

    public static String BitSetToString(BitSet bi, int size) {

        StringBuilder s = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (bi.get(i))
                s.append("1");
            else s.append("0");
        }
        return s.toString();
    }
}