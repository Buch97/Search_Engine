package it.unipi.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.BitSet;

import static java.lang.Math.log;

public class Compression {

    private final BitSet bitUnary;
    private final BitSet bitGamma;
    private final ByteArrayOutputStream variableByteBuffer;
    private int posUnary;
    private int formerElem = 0;
    private int posGamma;
    private int posVarByte;

    public Compression() {
        bitUnary = new BitSet();
        bitGamma = new BitSet();
        variableByteBuffer = new ByteArrayOutputStream();
        posUnary = 0;
        posGamma = 0;
        posVarByte = 0;
    }

    public static int convert(BitSet bits) {
        int value = 0;
        for (int i = 0; i < bits.length(); ++i) {
            value += bits.get(i) ? (1 << i) : 0;
        }
        return value;
    }

    public void encodingVariableByte(int n) throws IOException {
        int gap = n - formerElem;
        formerElem = n;
        if (n != 0) {
            int i = (int) (log(gap) / log(128)) + 1;
            byte[] rv = new byte[i];
            int j = i - 1;
            do {
                rv[j--] = (byte) (gap % 128);
                gap /= 128;
            } while (j >= 0);
            rv[i - 1] += 128;
            variableByteBuffer.write(rv);
        } else {
            variableByteBuffer.write(new byte[]{0});
        }
    }

    public int decodingVariableByte(byte[] byteStream) {
        int n = 0;
        int num = 0;
        for (int i = posVarByte; i < byteStream.length; i++) {
            if (byteStream[i] == 0 && n == 0) {
                posVarByte = ++i;
                return formerElem;
            } else if ((byteStream[i] & 0xff) < 128) {
                n = 128 * n + byteStream[i];
            } else {
                int gap = (128 * n + ((byteStream[i] - 128) & 0xff));
                posVarByte = ++i;
                num = gap + formerElem;
                formerElem = num;
                return num;
            }
        }
        return num;
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

        posGamma = i + sizebs;
        BitSet bs = bitSet.get(i + 1, posGamma);

        int gap = (int) (Math.pow(2, sizebs - 1) + convert(bs));
        int n = gap + formerElem;
        formerElem = n;
        return n;
    }

    public void unaryEncoding(int n) {
        bitUnary.set(posUnary, posUnary + n - 1);
        bitUnary.clear(posUnary + n - 1);
        posUnary += n;
    }

    public int decodingUnaryList(BitSet bitSet) {
        int count = bitSet.nextClearBit(posUnary) + 1 - posUnary;
        posUnary = posUnary + count;
        return count;
    }

    public BitSet getUnaryBitSet() {
        return bitUnary;
    }

    public BitSet getGammaBitSet() {
        return bitGamma;
    }

    public ByteArrayOutputStream getVariableByteBuffer() {
        return variableByteBuffer;
    }

    public int getPosUnary() {
        return posUnary;
    }

    public int getPosGamma() {
        return posGamma;
    }
}