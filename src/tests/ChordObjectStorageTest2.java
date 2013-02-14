package tests;

import chord.*;
/**
 * Tests that the group can form properly when peers get join requests
 * before they are connected themselves. Also tests that objects can
 * be stored and gotten simultaneously.
 */

public class ChordObjectStorageTest2 {

    public static void main(String[] _) {

        ChordObjectStorageImpl server[] = new ChordObjectStorageImpl[10];
        server[0] = new ChordObjectStorageImpl(-1); 
        server[0].createGroup(40000); 

        for (int i=1; i<10; i++) {
            server[i] = new ChordObjectStorageImpl(-1);
            server[i].joinGroup(server[i-1].getChordName(),40000+i);
        }

        for (int j=1; j<100; j++) {
            String name = "Name " + j;
            Object object = Integer.valueOf(j);
            int serverAtWhichToPut = j % 10;
            System.out.println("Putting " + j);
            server[serverAtWhichToPut].put(name, object);
        }

        /**
         * Let us give the puts a few second to arrive at their right
         * peers.
         */
        try {
            Thread.sleep(10000);
        } catch (InterruptedException __) {
        }

        boolean OK = true;

        for (int j=1; j<100; j++) {
            String name = "Name " + j;
            int serverAtWhichToGet = (3*j+5) % 10;
            System.out.println("Getting " + j);
            Object object = server[serverAtWhichToGet].get(name);
            if (object==null) {
                System.err.println("ERROR: " + j + " no object!");
                OK = false;
            } else {
                if (!(object instanceof Integer)) {
                    System.err.println("ERROR: " + j + " wrong type!");
                    OK = false;
                } else {
                    if (((Integer)object).intValue()!=j) {
                        System.err.println("ERROR: " + j + " wrong value!");
                        OK = false;
                    }
                }
            }
        }    

        if (OK) System.err.println("SUCCESS!");
        else System.err.println("Try again!");

        //for (int i=0; i<10; i++) {
        //    server[i].leaveGroup();
        //}


    }

}
