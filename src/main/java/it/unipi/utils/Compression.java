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

    public int gammaDecodingList(BitSet bitSet, int size) { 
        int i = posGamma;
        BitSet bs;

        while (i < size) {
            if (!bitSet.get(i))
                break;
            i++;
        }

        bs = new BitSet(i + 1 - posGamma);
        int sizebs = i + 1 - posGamma;

        bs.set(0, true);
        int cont = 1;
        while (cont < sizebs) {
            i++;
            bs.set(cont, bitSet.get(i));
            cont++;
        }
        posGamma = i + 1;

        int gap = Integer.parseInt(BitSetToString(bs, sizebs), 2);
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

    public int decodingUnaryList(BitSet bitSet, int size) {
        int count = 0;
        //System.out.println("posunary: " + posUnary);
        //System.out.println("size: " + size);
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

    public int getPosUnary() {
        return posUnary;
    }

    public int getPosGamma() {
        return posGamma;
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