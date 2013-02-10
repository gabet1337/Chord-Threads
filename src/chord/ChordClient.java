package chord;

import interfaces.*;
import java.net.*;
import java.util.Map;
import java.util.concurrent.*;

public class ChordClient implements Runnable {

    private BlockingQueue<Message> _incomingMessages;
    private BlockingQueue<Message> _outgoingMessages;

    private Map<Integer, ResponseHandler> _responseHandlers;

    private ChordObjectStorage _nodeReference;

    private boolean _isRunning;

    private Object _joiningLock = new Object();

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
        //THERE WILL BE SOME SYNCHRONIZATION HERE AT SOME POINT TO HANDLE MULTIPLE JOINS
        synchronized(_joiningLock) {
            ((ChordObjectStorageImpl)_nodeReference).lookupNoReturn(message);
        }
    }

    private void handleLookup(Message message) {
        _nodeReference.lookup(message);
        //ignore the result and have handle result forward it.
    }

    private void handleSetPredecessor(Message message) {
        synchronized(_joiningLock) {
            _nodeReference.setPredecessor((InetSocketAddress) message.payload);
        }
    }

    private void handleSetSuccessor(Message message) {
        synchronized(_joiningLock) {
            _nodeReference.setSuccessor((InetSocketAddress) message.payload);
        }
    }

    private void handleGetPredecessor(Message message) {
        synchronized(_joiningLock) {
            message.receiver = message.sender;
            message.sender = _nodeReference.getChordName();
            message.payload = _nodeReference.pred();
            message.type = Message.RESULT;
            enqueueMessage(message);
        }
    }

    private void handleGetSuccessor(Message message) {
        synchronized(_joiningLock) {
            message.receiver = message.sender;
            message.sender = _nodeReference.getChordName();
            message.payload = _nodeReference.succ();
            message.type = Message.RESULT;
            enqueueMessage(message);
        }
    }

    private void handleGetObject(Message message) {

    }

    private void handleSetObject(Message message) {

    }

    private void handleResult(Message message) {
        ResponseHandler handler = _responseHandlers.get(message.ID);
        handler.setMessage(message);
    }

    private Message getMessageToHandle() {
        Message message = _incomingMessages.poll();
        return message;
    }

    

    private void enqueueMessage(Message message) {
        _outgoingMessages.add(message);
    }

    public void stopClient() {
        _isRunning = false;
    }

}
