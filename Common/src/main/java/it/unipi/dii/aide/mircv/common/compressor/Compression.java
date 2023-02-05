package it.unipi.dii.aide.mircv.common.compressor;

import it.unipi.dii.aide.mircv.common.bean.Posting;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import static java.lang.Math.log;

public class Compression {

    private BitSet bitUnary;
    private ByteArrayOutputStream variableByteBuffer;
    private int posUnary;
    private int formerElem = 0;
    private Posting[] decodedPostingList;
    private int doc_freq;

    public Compression(int doc_frequency) {
        posUnary = 0;
        doc_freq = doc_frequency;
        decodedPostingList = new Posting[doc_freq];
    }

    public Compression(){
        bitUnary = new BitSet();
        variableByteBuffer = new ByteArrayOutputStream();
        posUnary = 0;
    }

    public static int convert(BitSet bits) {
        int value = 0;
        for (int i = 0; i < bits.length(); ++i) {
            value += bits.get(i) ? (1 << i) : 0;
        }
        return value;
    }

    public void decodePostingList(byte[] doc_id_buffer, byte[] term_freq_buffer) {
        int size = decodedPostingList.length;
        for (int i = 0; i < size; i++) {
            decodedPostingList[i] = new Posting();
        }

        decodingUnaryList(BitSet.valueOf(term_freq_buffer));
        decodingVariableByte(doc_id_buffer);
    }

    public void decodingVariableByte(byte[] byteStream) {
        int n = 0;
        int pos = 0;
        for (int item : byteStream) {
            if (item == 0 && n == 0) {
                decodedPostingList[pos].setDoc_id(formerElem);
                pos++;
            } else if ((item & 0xff) < 128) {
                n = 128 * n + item;
            } else {
                int gap = (128 * n + ((item - 128) & 0xff));
                formerElem = gap + formerElem;
                decodedPostingList[pos].setDoc_id(formerElem);
                pos++;
                n = 0;
            }
        }
    }

    public void decodingUnaryList(BitSet bitSet) {
        int i = 0;
        int pos = 0;
        while (i < doc_freq) {
            int count = bitSet.nextClearBit(pos) + 1 - pos;
            pos = pos + count;
            decodedPostingList[i].setTerm_frequency(count);
            i++;
        }
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

    public List<Posting> getDecodedPostingList() {
        return List.of(decodedPostingList);
    }

    public void unaryEncoding(int n) {
        bitUnary.set(posUnary, posUnary + n - 1);
        bitUnary.clear(posUnary + n - 1);
        posUnary += n;
    }

    public BitSet getUnaryBitSet() {
        return bitUnary;
    }

    public ByteArrayOutputStream getVariableByteBuffer() {
        return variableByteBuffer;
    }

    public int getPosUnary() {
        return posUnary;
    }

    /*public int getPosGamma() {
        return posGamma;
    }

    public BitSet getGammaBitSet() {
        return bitGamma;
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
    }*/

}