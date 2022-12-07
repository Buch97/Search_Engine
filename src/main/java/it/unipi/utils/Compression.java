package it.unipi.utils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class Compression {

    public List<BitSet> listEncodingUnary(List<Integer> n){
        List<BitSet> bs=new ArrayList<BitSet>(n.size());
        bs.add(unaryEncoding(n.get(0)));

        for(int i=1;i<n.size();i++)
            bs.add(unaryEncoding(n.get(i)-n.get(i-1)));

        return bs;
    }

    public List<Integer> listDecodingUnary(List<BitSet> bs){
        List<Integer> n=new ArrayList<Integer>(bs.size());
        n.add(unaryDecoding(bs.get(0)));

        for(int i=1;i<n.size();i++)
            n.add(unaryDecoding(bs.get(i))+unaryDecoding(bs.get(i-1)));

        return n;
    }

    public BitSet gammaEncoding(int n){
        BitSet binN=intToBitSet(n);
        BitSet b = unaryEncoding(binN.size());
        BitSet bitSet=new BitSet(2*binN.size()-1);
        for(int i=0;i<bitSet.size();i++){
            if(i<binN.size()){
                bitSet.set(i,b.get(i));
            }else{
                bitSet.set(i,binN.get(b.size()-2+i-3));
            }
        }
        return bitSet;
    }

    public int gammaDecoding(BitSet bitSet){
        int i=0;
        BitSet bs;
        while(i<bitSet.size()){
            if(bitSet.get(i)==false)
                break;
            
            i++;
        }
        bs=new BitSet(bitSet.size()-i);
        bs.set(0,true);
        int cont=1;
        while(cont<bs.size()){
            i++;
            bs.set(cont,bitSet.get(i));
            cont++;
        }
        return toInt(bs);

    }

    public BitSet unaryEncoding(int n){
        BitSet bitSet = new BitSet(n+1);
        bitSet.set(0,n,true);
        bitSet.set(n,false);
        return bitSet;
    }

    public int unaryDecoding(BitSet bitSet){
        return bitSet.cardinality()+1;
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

    
}
