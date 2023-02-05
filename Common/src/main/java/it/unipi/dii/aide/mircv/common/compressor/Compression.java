package it.unipi.dii.aide.mircv.common.compressor;

import it.unipi.dii.aide.mircv.common.bean.Posting;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import static java.lang.Math.log;

/* Implement the unary compressor used to compress the frequencies and
   the variable byte used to compress the documents id in the inverted index.
 */
public class Compression {

    private BitSet bitUnary;                            // Sequence of bits in unary encoding
    private ByteArrayOutputStream variableByteBuffer;   //Buffer of bytes in variable byte encoding
    private int posUnary;                               //Position of the next bit in bitUnary to encode a frequency
    private int formerElem = 0;                         //doc id previous to the current one to compress, used for the D-gap
    private Posting[] decodedPostingList;               //Array of Postings of the term decompressed
    private int doc_freq;

    //Constructor for decompression of a term's posting list
    public Compression(int doc_frequency) {
        posUnary = 0;
        doc_freq = doc_frequency;
        decodedPostingList = new Posting[doc_freq];
    }

    //Constructor for compression of a doc_id and term frequency of a posting
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


    //inizialize and decode the sequence of posting of a term
    public void decodePostingList(byte[] doc_id_buffer, byte[] term_freq_buffer) {
        int size = decodedPostingList.length;
        for (int i = 0; i < size; i++) {
            decodedPostingList[i] = new Posting();
        }

        decodingUnaryList(BitSet.valueOf(term_freq_buffer));
        decodingVariableByte(doc_id_buffer);
    }

    // Decode the stream of bytes of doc id of the posting lists compressed to integers
    public void decodingVariableByte(byte[] byteStream) {
        int n = 0;
        int pos = 0;                    //index of the next doc id decompressed
        for (int item : byteStream) {    //takes 4 bytes at time from byteStream
            if (item == 0 && n == 0) {   //the value to decompress is zero
                decodedPostingList[pos].setDoc_id(formerElem);
                pos++;
            } else if ((item & 0xff) < 128) {           //The MSB is zero
                n = 128 * n + item;                     // not the termination byte, shift the actual number and insert the new byte
            } else {                                    //The MSB is 1, is the last byte of the number compressed
                int gap = (128 * n + ((item - 128) & 0xff));     // termination byte, remove the 1 at the MSB and then append the byte to the number
                formerElem = gap + formerElem;                  //retrieve the current doc id from the gap
                decodedPostingList[pos].setDoc_id(formerElem);
                pos++;
                n = 0;
            }
        }
    }

    //Decode the sequence of bits of term frequencies of the posting lists compressed to integers
    public void decodingUnaryList(BitSet bitSet) {
        int i = 0;
        int pos = 0;
        while (i < doc_freq) {    //decode all the posting
            int count = bitSet.nextClearBit(pos) + 1 - pos;
            pos = pos + count;                  //update to the next number to decode
            decodedPostingList[i].setTerm_frequency(count);
            i++;
        }
    }


    //encode an integer to variable byte
    public void encodingVariableByte(int n) throws IOException {
        int gap = n - formerElem;               /* compute the difference between the current number and the previous one,
                                                   encode the gap */

        formerElem = n;                         // save the current integer for the next compression
        if (n != 0) {
            int i = (int) (log(gap) / log(128)) + 1;    //compute the number of byte needed to compress the integer
            byte[] rv = new byte[i];                    // allocate the output byte array
            int j = i - 1;
            do {
                rv[j--] = (byte) (gap % 128);
                gap /= 128;
            } while (j >= 0);
            rv[i - 1] += 128;       // set the most significant bit of the least significant byte to 1
            variableByteBuffer.write(rv);
        } else {
            variableByteBuffer.write(new byte[]{0});
        }
    }

    public List<Posting> getDecodedPostingList() {
        return List.of(decodedPostingList);
    }

    //encode an integer to unary encoding.
    public void unaryEncoding(int n) {
        bitUnary.set(posUnary, posUnary + n - 1);
        bitUnary.clear(posUnary + n - 1);
        posUnary += n;                              //update the position of the sequence to the first bit available.
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