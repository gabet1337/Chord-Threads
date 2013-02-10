package chord;

import interfaces.*;
import java.net.*;
import java.util.Map;
import java.util.concurrent.*;

import services.ChordHelpers;

public class ChordClient implements Runnable {

    private BlockingQueue<Message> _incomingMessages;
    private BlockingQueue<Message> _outgoingMessages;
    private Map<Integer, ResponseHandler> _responseHandlers;
    private Map<String, Object> _localStore;
    private ChordObjectStorage _nodeReference;
    private boolean _isRunning;
    private Object _joiningLock = new Object();

    public ChordClient(BlockingQueue<Message> incoming, BlockingQueue<Message> outgoing, 
            Map<Integer, ResponseHandler> responseHandlers , ChordObjectStorage node,
            Map<String, Object> localStore) {
        _incomingMessages = incoming;
        _outgoingMessages = outgoing;
        _responseHandlers = responseHandlers;
        _nodeReference = node;
        _localStore = localStore;
        _isRunning = true;
    }

    public void run() {

        while (_isRunning) {

            Message message = getMessageToHandle();
            if (message != null) {
//                System.out.println(message);
                switch (message.type) {
                case Message.JOIN : handleJoin(message); break;
                case Message.LOOKUP : handleLookup(message); break;
                case Message.SET_PREDECESSOR : handleSetPredecessor(message); break;
                case Message.SET_SUCCESSOR : handleSetSuccessor(message); break;
                case Message.GET_PREDECESSOR : handleGetPredecessor(message); break;
                case Message.GET_SUCCESSOR : handleGetSuccessor(message); break;
                case Message.GET_OBJECT : handleGetObject(message); break;
                case Message.SET_OBJECT : handlePutObject(message); break;
                case Message.RESULT : handleResult(message); break;
                default : System.err.println("Invalid message received. Ignore it");
                }
            }
        }
        
        System.out.println("Client stopped");

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
        if (iAmResponsibleForThisKey(message.key)) {
            //find the key in my localstore and send it back to
            //the origin which will trigger an event on the synchronous waiter
            message.payload = _localStore.get(message.name);
            message.receiver = message.origin;
            message.type = Message.RESULT;
            message.sender = _nodeReference.getChordName();
            enqueueMessage(message);
            //System.out.println("Im sending an object back");
        } else {
            //well, lets see if my successor want anything to do with this
            message.sender = _nodeReference.getChordName();
            message.receiver = _nodeReference.succ();
            enqueueMessage(message);
        }
    }

    private void handlePutObject(Message message) {
        if (iAmResponsibleForThisKey(message.key)) {
            //Input this key into my localstore
            _localStore.put(message.name, message.payload);
            //System.out.println("OBJECT: " + message.name + " STORED AT: " + _nodeReference);
        } else {
            //send it along to my successor
            message.sender = _nodeReference.getChordName();
            message.receiver = _nodeReference.succ();
            enqueueMessage(message);
        }
    }

    private void handleResult(Message message) {
        ResponseHandler handler = _responseHandlers.get(message.ID);
        handler.setMessage(message);
    }

    private Message getMessageToHandle() {
        Message message = null;
        try {
            message = _incomingMessages.poll(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("We were interrupted while waiting for messages!");
        }
        return message;
    }

    private void enqueueMessage(Message message) {
        _outgoingMessages.add(message);
    }

    public void stopClient() {
        _isRunning = false;
        System.out.println("Stopping client!");
        Thread.currentThread().interrupt();
    }
    
    private boolean iAmResponsibleForThisKey(int key) {
        int low = ChordHelpers.keyOfObject(_nodeReference.pred());
        int high = ChordHelpers.keyOfObject(_nodeReference.getChordName());
        return ChordHelpers.inBetween(low, high, key);
    }

}
