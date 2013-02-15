package chord;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class ChordMessageSender implements Runnable {

    private BlockingQueue<Message> _outgoingMessages;

    private boolean _isRunning;
    ChordObjectStorageImpl _nodeReference;

    public ChordMessageSender(BlockingQueue<Message> outgoing, ChordObjectStorageImpl nodeReference) {
        _outgoingMessages = outgoing;
        _isRunning = true;
        _nodeReference = nodeReference;
    }

    public void run() {
        synchronized (_outgoingMessages) {
            while (_isRunning) {

                while (!_outgoingMessages.isEmpty()) {
                    Message msg = null;
                    try {
                        msg = _outgoingMessages.take();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    _nodeReference.debug("sending message " + msg.getTypeString() + " to " + msg.receiver.getPort());
                    Socket s = getSocket(msg.receiver);
                    sendMessage(s, msg);
                    //                System.out.println("Sent message: " + msg);
                    try {
                        s.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    _outgoingMessages.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            _nodeReference.debug("Messenger stopped");
        }
    }

    private void sendMessage(Socket socket, Message msg) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(msg);
            oos.flush();
            oos.close();
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
        _nodeReference.debug("Stopping messenger");
    }

}
