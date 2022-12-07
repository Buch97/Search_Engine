package it.unipi.utils;

public class AuxObject {
    private static int posG;
    private static int posU;

    public AuxObject(int p){
        posU=p;
        posG=p;
    }
    
    public int getPosG(){
        return posG;
    }

    public int getPosU(){
        return posU;
    }

    public static void setPosG(int p){
        posG=p;
    }

    public static void setPosU(int p){
        posU=p;
    }
}
