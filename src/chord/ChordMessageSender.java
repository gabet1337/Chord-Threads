package chord;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class ChordMessageSender implements Runnable {
  
    private boolean _isRunning;

    private ChordObjectStorageImpl _nodeReference;
    
    public ChordMessageSender(ChordObjectStorageImpl node) {
        _nodeReference = node;
        _isRunning = true;
    }

    public void run() {

        while (_isRunning) {

            while (!_nodeReference.getOutgoingMessages().isEmpty()) {
                Message msg = null;
                try {
                    msg = _nodeReference.getOutgoingMessages().take();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                Socket s = getSocket(msg.receiver);
                sendMessage(s, msg);
//                System.out.println("Sent message: " + msg);
                try {
                    s.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Messenger stopped");
    }

    private void sendMessage(Socket socket, Message msg) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(msg);
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Socket getSocket(InetSocketAddress receiver) {
        Socket result = new Socket();
        try {
            result.connect(receiver);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    
    public void stopSender() {
        _isRunning = false;
        System.out.println("Stopping messenger");
    }

}
