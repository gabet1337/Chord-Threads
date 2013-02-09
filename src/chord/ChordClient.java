package chord;

import interfaces.ChordObjectStorage;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.*;

public class ChordClient implements Runnable {

    private BlockingQueue<Message> _incomingMessages;

    private ChordObjectStorage _nodeReference;
    
    private boolean _isRunning;

    public ChordClient(BlockingQueue<Message> incoming, ChordObjectStorage node) {
        _incomingMessages = incoming;
        _nodeReference = node;
        _isRunning = true;
    }

    public void run() {

        while (_isRunning) {
            Message message = getMessageToHandle();

            switch (message.type) {
            case Message.JOIN : handleJoin(message); break;
            case Message.LOOKUP : handleLookup(message); break;
            case Message.SET_PREDECESSOR : handleSetPredecessor(message); break;
            case Message.SET_SUCCESSOR : handleSetSuccessor(message); break;
            case Message.GET_PREDECESSOR : handleGetPredecessor(message); break;
            case Message.GET_SUCCESSOR : handleGetSuccessor(message); break;
            case Message.GET_OBJECT : handleGetObject(message); break;
            case Message.SET_OBJECT : handleSetObject(message); break;
            case Message.RESULT : handleResult(message); break;
            default : System.err.println("Invalid message received. Ignore it");
            }

        }

    }

    private void handleJoin(Message message) {

    }

    private void handleLookup(Message message) {

    }

    private void handleSetPredecessor(Message message) {

    }

    private void handleSetSuccessor(Message message) {

    }

    private void handleGetPredecessor(Message message) {

    }

    private void handleGetSuccessor(Message message) {

    }

    private void handleGetObject(Message message) {

    }

    private void handleSetObject(Message message) {

    }
    
    private void handleResult(Message message) {
        
    }

    private Message getMessageToHandle() {
        try {
            Message message = _incomingMessages.take();
            return message;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
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
    
    public void stopClient() {
        _isRunning = false;
    }

}
