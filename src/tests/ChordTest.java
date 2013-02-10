package tests;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import services.ChordHelpers;

import chord.*;

public class ChordTest {

    public static void main(String[] args) throws InterruptedException, IOException {
        //testReceive();
        //testLookup1Node();
        testJoin1Node();
    }

    private static void testReceive() throws InterruptedException, IOException {
        ChordObjectStorageImpl node = new ChordObjectStorageImpl(-1);
        node.createGroup(40000);
        Thread.sleep(200);
        Message msg = new Message(Message.JOIN, 1000, new InetSocketAddress("localhost", 40001),
                new InetSocketAddress("localhost", 40001), node.getChordName(), null);
        Socket s = new Socket();
        s.connect(node.getChordName());
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        oos.writeObject(msg);
        oos.flush();
        s.close();
        Thread.sleep(100);
    }

    private static void testLookup1Node() throws InterruptedException {
        ChordObjectStorageImpl node = new ChordObjectStorageImpl(-1);
        node.createGroup(40000);
        Thread.sleep(200);
        Message message = new Message(Message.LOOKUP, ChordHelpers.keyOfObject(node),
                node.getChordName(), node.getChordName(), node.getChordName(), null);
        System.out.println(node.lookup(message));
    }

    private static void testJoin1Node() throws InterruptedException {
        ChordObjectStorageImpl node = new ChordObjectStorageImpl(-1);
        node.createGroup(40000);
        Thread.sleep(200);
        ChordObjectStorageImpl node1 = new ChordObjectStorageImpl(-1);
        node1.joinGroup(node.getChordName(), 40001);
        Thread.sleep(200);
    }

}
