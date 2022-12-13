package it.unipi.utils;

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

        bs = new BitSet(i);
        bs.set(0, true);
        int cont = 1;
        while (cont < size - i) {
            i++;
            bs.set(cont, bitSet.get(i));
            cont++;
        }
        posGamma = i;
        return toInt(bs);
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

    public int toInt(BitSet bitSet) {
        int intValue = 0;
        for (int bit = 0; bit < bitSet.length(); bit++) {
            if (bitSet.get(bit)) {
                intValue |= (1 << bit);
            }
        }
        return intValue;
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
}