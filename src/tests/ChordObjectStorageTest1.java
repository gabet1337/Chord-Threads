package tests;

import chord.*;

/**
 * Tests that the group can form properly when peers do not get join
 * requests until they are connected themselves. Also tests that
 * objects can be stored and retrieved, when there are due time between
 * the calls. The details are there to avoid errors due to bad
 * handling of concurrency.
 */

public class ChordObjectStorageTest1 {

    public static void main(String[] _) {

        ChordObjectStorageImpl server[] = new ChordObjectStorageImpl[10];

        server[0] = new ChordObjectStorageImpl(-1); 
        server[0].createGroup(40000); 

        while (!server[0].isConnected()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException __) {
            }
        }

        for (int i=1; i<10; i++) {
            server[i] = new ChordObjectStorageImpl(-1);
            server[i].joinGroup(server[i-1].getChordName(),40000+i);
            while (!server[0].isConnected()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException __) {
                }
            }
        }

        for (int j=1; j<100; j++) {
            String name = "Name " + j;
            Object object = Integer.valueOf(j);
            int serverAtWhichToPut = (3*j+5) % 10;
            System.out.println("Putting " + j);
            server[serverAtWhichToPut].put(name, object);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException __) {
            }	
        }

        boolean OK = true;

        for (int j=1; j<100; j++) {
            String name = "Name " + j;
            int serverAtWhichToGet = j % 10;
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

        for (int i=0; i<10; i++) {
            server[i].leaveGroup();
        }


    }

}
