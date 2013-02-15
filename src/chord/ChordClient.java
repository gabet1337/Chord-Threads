package chord;

import interfaces.*;
import java.net.*;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import services.ChordHelpers;

public class ChordClient implements Runnable {

    private ChordObjectStorageImpl _nodeReference;
    private boolean _isRunning;
    private Object _joiningLock = new Object();

    private boolean _deferMessages = false;

    public ChordClient(ChordObjectStorageImpl node) {
        _nodeReference = node;
        //_localStore = localStore;
        _isRunning = true;
    }

    Random generator = new Random();

    public void run() {

        while (_isRunning) {

            Message message = getMessageToHandle();
            if (message != null) {

                if (_nodeReference._guestLock && (message.type != Message.SET_OBJECT && message.type != Message.LOCK && message.type != Message.SET_SUCCESSOR
                        && message.type != Message.SET_PREDECESSOR)) {
                    _nodeReference._incomingMessages.add(message);
                    _nodeReference.debug("Will not process message " + message.type + " because im blocked on guestlock");
                } else if (_nodeReference._selfLock && (message.type != Message.RESULT)) {
                    _nodeReference._incomingMessages.add(message);
                    _nodeReference.debug("Will not process message " + message.type + " because im blocked on selflock");
                } else if (!_nodeReference._isConnected && (message.type != Message.RESULT)) {
                    _nodeReference._incomingMessages.add(message);
                    _nodeReference.debug("Will not process message " + message.type + " because i am currently blocked while joining.");
                } else {
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
                    case Message.MIGRATE : handleMigrate(message); break;
                    case Message.LOCK : handleLock(message); break;
                    case Message.UNLOCK : handleUnlock(message); break;
                    default : System.err.println("Invalid message received. Ignore it");
                    }
                }
            }
        }

        _nodeReference.debug("Client stopped");

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
            _nodeReference.enqueueMessage(message);
        }
    }

    private void handleGetSuccessor(Message message) {
        synchronized(_joiningLock) {
            message.receiver = message.sender;
            message.sender = _nodeReference.getChordName();
            message.payload = _nodeReference.succ();
            message.type = Message.RESULT;
            _nodeReference.enqueueMessage(message);
        }
    }

    private void handleGetObject(Message message) {
        if (iAmResponsibleForThisKey(message.key)) {
            //find the key in my localstore and send it back to
            //the origin which will trigger an event on the synchronous waiter
            message.payload = _nodeReference._localStore.get(message.name);
            message.receiver = message.origin;
            message.type = Message.RESULT;
            message.sender = _nodeReference.getChordName();
            _nodeReference.enqueueMessage(message);
            //System.out.println("Im sending an object back");
        } else {
            //well, lets see if my successor want anything to do with this
            message.sender = _nodeReference.getChordName();
            message.receiver = _nodeReference.succ();
            _nodeReference.enqueueMessage(message);
        }
    }

    private void handlePutObject(Message message) {
        if (iAmResponsibleForThisKey(message.key)) {
            //Input this key into my localstore
            _nodeReference._localStore.put(message.name, message.payload);
            _nodeReference.debug("I just added " + message.name + " to my localStore. By the way; my localStore now contains: " + _nodeReference._localStore.toString());
            //System.out.println("OBJECT: " + message.name + " STORED AT: " + _nodeReference);
        } else {
            //send it along to my successor
            message.sender = _nodeReference.getChordName();
            message.receiver = _nodeReference.succ();
            _nodeReference.enqueueMessage(message);
        }
    }

    private void handleResult(Message message) {
        ResponseHandler handler = _nodeReference._responseHandlers.get(message.ID);
        handler.setMessage(message);
        _nodeReference._responseHandlers.remove(message.ID);
    }

    private void handleMigrate(Message message) {
        //ADD CODE HERE TO HANDLE MIGRATE!
        //Upon receiving a request for migrate I should do the following:
        //1. Check the payload to get the predecessors key.
        //2. Get the key of the sender of the migrate message
        //3. Send objects with keys having the following property:
        // keyOfObject(predecessor) > key >= keyObObject(sender)
        // Message should be of type: SET_OBJECT.

        int lowKey = ChordHelpers.keyOfObject((InetSocketAddress)message.payload);
        int highKey = ChordHelpers.keyOfObject(message.sender);

        _nodeReference.migrate(lowKey, highKey);

    }

    private void handleLock(Message message) {
        //        boolean status = this.lock();
        //        message.payload = new Boolean(status);
        //        message.receiver = message.origin;
        //        message.sender = _nodeReference.getChordName();
        //        message.type = Message.RESULT;
        //        _nodeReference.enqueueMessage(message);
        synchronized (_nodeReference._LOCK) {
            if (_nodeReference._selfLock || _nodeReference._guestLock) {
                message.payload = new Boolean(false);
            } else {
                _nodeReference._guestLock = true;
                message.payload = new Boolean(true);
            }
            message.receiver = message.origin;
            message.sender = _nodeReference.getChordName();
            message.type = Message.RESULT;
            _nodeReference.enqueueMessage(message);
        }
    }

    private void handleUnlock(Message message) {
        _nodeReference._guestLock = false;
    }

    private Message getMessageToHandle() {
        Message message = null;
        try {
            message = _nodeReference._incomingMessages.poll(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("We were interrupted while waiting for messages!");
        }
        return message;
    }

    public void stopClient() {
        _isRunning = false;
        _nodeReference.debug("Stopping client!");
    }

    private boolean iAmResponsibleForThisKey(int key) {
        int low = ChordHelpers.keyOfObject(_nodeReference.pred());
        int high = ChordHelpers.keyOfObject(_nodeReference.getChordName());
        return ChordHelpers.inBetween(low, high, key);
    }


    public synchronized boolean lock() {
        if (_deferMessages) {
            return false;
        } else {
            _deferMessages = true;
            return true;
        }
    }

    public synchronized void unlock() {
        _deferMessages = false;
    }

}
