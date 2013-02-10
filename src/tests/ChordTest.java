package tests;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import services.ChordHelpers;

import chord.*;

public class ChordTest {

    public static void main(String[] args) throws InterruptedException, IOException {
        //testReceive();
        //testLookup1Node();
        //testJoin1Node();
        testJoin2Node();
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
        System.out.println(node.toString());
        System.out.println(node1.toString());
    }
    
    private static void testJoin2Node() throws InterruptedException {
        ArrayList<ChordObjectStorageImpl> nodes = new ArrayList<ChordObjectStorageImpl>();
        ChordObjectStorageImpl node = new ChordObjectStorageImpl(-1);
        nodes.add(node);
        node.createGroup(40000);

        Thread.sleep(200);
        ChordObjectStorageImpl node1 = new ChordObjectStorageImpl(-1);
        nodes.add(node1);
        node1.joinGroup(node.getChordName(), 40001);
        
        ChordObjectStorageImpl node2 = new ChordObjectStorageImpl(-1);
        nodes.add(node2);
        node2.joinGroup(node.getChordName(), 40002);
        Thread.sleep(1000);
        System.out.println(node.toString());
        System.out.println(node1.toString());
        System.out.println(node2.toString());
        System.out.println(graph(nodes));
    }
    
    private static String graph(List<ChordObjectStorageImpl> nodes) {
        String result = "";
        result += "digraph test {\n";
        result += "size=\"50,50\" \n";
        result += "layout=\"neato\"\n";
        result += "nodesep=\"1\"\n";
        result += "ranksep=\"2\"\n";
        
        for (ChordObjectStorageImpl node : nodes) {
            result += ChordHelpers.keyOfObject(node.getChordName()) + " [color=none; shape=plaintext; fontsize=10];\n";
        }
        
        for (ChordObjectStorageImpl node : nodes) {
            result += node.getGraphViz();
        }

        result += "}";
        return result;
    }

}
