package chord;

import interfaces.*;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.concurrent.*;

public class ChordClient implements Runnable {

    private BlockingQueue<Message> _incomingMessages;
    private BlockingQueue<Message> _outgoingMessages;

    private Map<Integer, ResponseHandler> _responseHandlers;

    private ChordObjectStorage _nodeReference;

    private boolean _isRunning;

    public ChordClient(BlockingQueue<Message> incoming, BlockingQueue<Message> outgoing, 
            Map<Integer, ResponseHandler> responseHandlers , ChordObjectStorage node) {
        _incomingMessages = incoming;
        _outgoingMessages = outgoing;
        _responseHandlers = responseHandlers;
        _nodeReference = node;
        _isRunning = true;
    }

    public void run() {

        while (_isRunning) {

            //First send any queued messages:
            while (!_outgoingMessages.isEmpty()) {
                Message msg = _outgoingMessages.poll();
                Socket s = getSocket(msg.receiver);
                sendMessage(s, msg);
                try {
                    s.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            Message message = getMessageToHandle();
            if (message != null) {
                System.out.println(message);
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

    }

    private void handleJoin(Message message) {

    }

    private void handleLookup(Message message) {

    }

    private void handleSetPredecessor(Message message) {
        _nodeReference.setPredecessor((InetSocketAddress) message.payload);
    }

    private void handleSetSuccessor(Message message) {
        _nodeReference.setSuccessor((InetSocketAddress) message.payload);
    }

    private void handleGetPredecessor(Message message) {
        message.receiver = message.origin;
        message.payload = _nodeReference.pred();
        message.type = Message.RESULT;
        enqueueMessage(message);
    }

    private void handleGetSuccessor(Message message) {
        message.receiver = message.origin;
        message.payload = _nodeReference.succ();
        message.type = Message.RESULT;
        enqueueMessage(message);
    }

    private void handleGetObject(Message message) {

    }

    private void handleSetObject(Message message) {

    }

    private void handleResult(Message message) {
        if (message.origin.equals(_nodeReference.getChordName())) {
            //we have been expecting this message. Lets find the response handler
            ResponseHandler handler = _responseHandlers.get(message.ID);
            handler.setMessage(message);
        } else {
            //send it to our predecessor
            message.receiver = _nodeReference.pred();
            enqueueMessage(message);
        }
    }

    private Message getMessageToHandle() {
        Message message = _incomingMessages.poll();
        return message;
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

    private void enqueueMessage(Message message) {
        try {
            _outgoingMessages.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stopClient() {
        _isRunning = false;
    }

}
