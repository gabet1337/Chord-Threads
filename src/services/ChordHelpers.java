package services;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

public class ChordHelpers {

    /**
     * Compute the key of a given object. Returns a positive 31-bit
     * integer, hashed as to be "random looking" even for similar
     * objects.
     */
    public static int keyOfObject(Object o)  {

        int h = o.hashCode();
        /* 
         * In the odd case that we do note have access to SHA-256, we
         * just use h as the key.
         */
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException _){
            System.err.println("Cannot use SHA-256, expect worse " +
                    "random-lookingness of the keys!");
            md = null;
        }

        if (md == null) {
            if (h < 0) h=-h;
            return h;
        }
        /*
         * When we have access to SHA-256, we use it to hash the
         * hashCode for better random-lookingness.
         */
        byte[] hAsBytes = ByteBuffer.allocate(4).putInt(h).array();


        md.reset();
        md.update(hAsBytes);
        byte[] longRandomLookingKey = md.digest();

        int[] keyAsInts = new int[4];
        for (int i=0; i<4; i++) {
            keyAsInts[i] = longRandomLookingKey[i];
            if (keyAsInts[i] < 0) keyAsInts[i]+=256;
        }

        long resAsLong = keyAsInts[3];
        resAsLong = resAsLong << 24;
        resAsLong = resAsLong |  
                (keyAsInts[2] << 16) | 
                (keyAsInts[1] << 8) |
                (keyAsInts[0] << 0) ;
        long twoToThe31 = 1073741824*2;
        resAsLong = resAsLong % twoToThe31;
        return (int)resAsLong;
    }

    /**
     * @param low Must be a positive 31-bit integer
     * @param high Must be a positive 31-bit integer
     * @param candidate Must be a positive 31-bit integer
     *       
     * Test if the candidate is in the interval [low, high-1].  If low
     * > high, then the question is answered with "wrap-around modulo
     * 2^31".
     */
    public static boolean inBetween(int low, int high, int candidate) {
        System.out.println("Is: " + low + " < " + candidate + " < " + high);
        boolean answer = false;
        if (low==high) answer = false;
        if ((candidate >= low) && (candidate < high)) answer = true;
        if (low > high) answer = true;
        System.out.println(answer);
        return answer;
    } 

    /**
     * @param a Must be a non-negative
     * @param b Must be a non-negative     
     * 
     * Computes a+b modulo 2^31 and returns it in an int.     
     */    
    public static int add(int a, int b) {
        /* We go to long to have plenty of space to add small positive
         * numbers, then we do the modulo reduction and go back to int
         * when we know we have a positive 31-bit number.
         */
        long A = a;
        long B = b;
        long resAsLong = A+B;
        long twoToThe31 = 2*1073741824;
        resAsLong = resAsLong % twoToThe31;
        int resAsInt = (int)resAsLong;
        return resAsInt;
    }

    /**
     * Computes the name of this peer by resolving the local host name
     * and adding the current portname.
     */
    public static InetSocketAddress getMyName(int port) {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            InetSocketAddress name = new InetSocketAddress(localhost, port);
            return name;
        } catch (UnknownHostException e) {
            System.err.println("Cannot resolve the Internet address " + 
                    "of the local host.");
            System.err.println(e);
        }
        return null;
    }

    public static ServerSocket getServerSocket(int port) {
        try {
            ServerSocket result = new ServerSocket();
            InetSocketAddress myAddress = new InetSocketAddress(InetAddress.getLocalHost(), port);
            result.bind(myAddress);
            return result;
        } catch (IOException e) {
            System.err.println("Could not create the server socket!");
            e.printStackTrace();
        }
        return null;
    }

}
