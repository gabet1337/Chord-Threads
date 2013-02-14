package chord;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import services.*;
import interfaces.*;

public class ChordObjectStorageImpl extends DDistThread implements ChordObjectStorage {

    private boolean _isJoining;
    private boolean _isConnected;
    private boolean _wasConnected;
    private int _port;
    private int _myKey;
    private InetSocketAddress _myName;
    private InetSocketAddress _succ;
    private InetSocketAddress _pred;
    private InetSocketAddress _connectedAt;

    private ChordClient _chordClient;
    private ChordServer _chordServer;
    private ChordMessageSender _chordMessenger;

    private Object _connectedLock = new Object();

    private BlockingQueue<Message> _incomingMessages = new LinkedBlockingQueue<Message>();
    private BlockingQueue<Message> _outgoingMessages = new LinkedBlockingQueue<Message>();

    private Map<Integer, ResponseHandler> _responseHandlers = 
            Collections.synchronizedMap(new HashMap<Integer ,ResponseHandler>());

    private Map<String, Object> _localStore = 
            Collections.synchronizedMap(new HashMap<String ,Object>());

    public ChordObjectStorageImpl(int expectedLifeSpan) {
        super(expectedLifeSpan);
        _isConnected = false;
        _wasConnected = false;
    }

    public int getPort() {
    	return _port;
    }
    
    public Map<String, Object> getLocalStore() {
    	return _localStore;
    }
    
    public Map<Integer, ResponseHandler> getResponseHandlers() {
    	return _responseHandlers;
    }
    
    public BlockingQueue<Message> getIncomingMessages() {
    	return _incomingMessages;
    }
    
    public BlockingQueue<Message> getOutgoingMessages() {
    	return _outgoingMessages;
    }
    
    public void createGroup(int port) {
        synchronized(_connectedLock) {
            if (_wasConnected) {
                System.err.println(getName() + " says: Cannot connect twice!");
                return;
            } 
            _wasConnected = true;
        }

        _isJoining = false;
        _port = port;
        _myName = ChordHelpers.getMyName(port);
        _myKey = ChordHelpers.keyOfObject(_myName);
        start();
    }

    public void joinGroup(InetSocketAddress knownPeer, int port) {
        synchronized(_connectedLock) {
            if (_wasConnected) {
                System.err.println(getName() + " says: Cannot connect twice!");
                return;
            } 
            _wasConnected = true;
        }

        _isJoining = true;
        _port = port;
        _connectedAt = knownPeer;
        _myName = ChordHelpers.getMyName(port);
        _myKey = ChordHelpers.keyOfObject(_myName);
        start();
    }

    public boolean isConnected() {
        synchronized(_connectedLock) {
            return _isConnected;
        }
    }

    public InetSocketAddress getChordName() {
        return _myName;
    }

    public void leaveGroup() {
        System.out.println("Shutting down!");
        synchronized(_connectedLock) {
            _isConnected = false;
        }

    }

    public InetSocketAddress succ() {
        return _succ;
    }

    public void setSuccessor(InetSocketAddress i) {
        _succ = i;
    }

    public void setPredecessor(InetSocketAddress i) {
        _pred = i;
    }

    public InetSocketAddress pred() {
        return _pred;
    }

    public void lookupNoReturn(Message message) {
        if (pred().equals(_myName) || ChordHelpers.inBetween(ChordHelpers.keyOfObject(pred()), _myKey, message.key)) {
            //send the message to origin
            message.receiver = message.origin;
            message.sender = getChordName();
            message.payload = getChordName();
            message.type = Message.RESULT;

            //lock ourself
            _chordClient.lock();
        } else {
            //forward to successor
            message.receiver = succ();
            message.sender = getChordName();
        }

        _outgoingMessages.add(message);
    }

    public InetSocketAddress lookup(Message message) {

        if (pred().equals(_myName)) {
            return _myName;
        }

        if (ChordHelpers.inBetween(_myKey, ChordHelpers.keyOfObject(_succ), message.key)) {
            return _succ;
        }
        System.out.println("HERE am I: " + _myName);
        //It's not in our proximity. Let's ask our successor.
        message.receiver = succ();
        message.sender = getChordName();
        MessageHandler handler = new MessageHandler();
        _responseHandlers.put(message.ID, handler);
        _outgoingMessages.add(message);
        return (InetSocketAddress) handler.getMessage().payload;
    }

    public void put(String name, Object object) {
        int key = ChordHelpers.keyOfObject(name);
        Message message = new Message(Message.SET_OBJECT, key, getChordName(),
                getChordName(), succ(), object);
        message.name = name;
        _outgoingMessages.add(message);
    }

    public Object get(String name) {
        //lets first see if we're holding it.
        Object result = _localStore.get(name);
        if (result != null)
            return result;

        int key = ChordHelpers.keyOfObject(name);
        Message message = new Message(Message.GET_OBJECT, key, getChordName(),
                getChordName(), succ(), null);
        message.name = name;
        MessageHandler handler = new MessageHandler();
        _responseHandlers.put(message.ID, handler);
        _outgoingMessages.add(message);
        result = handler.getMessage().payload;
        return result;
    }

    public String toString() {
        String result = "NODEID: " + _myKey + "\n" +
                "ADDR: " + _myName + "\n" +
                "SUCC: " + succ() + "\n" +
                "PRED: " + pred();
        return result;
    }

    public void run() {
        _chordServer = new ChordServer(this);
        _chordClient = new ChordClient(this);
        _chordMessenger = new ChordMessageSender(this);

        Thread server = new Thread(_chordServer);
        Thread client = new Thread(_chordClient);
        Thread messenger = new Thread(_chordMessenger);

        server.start();
        client.start();
        messenger.start();

        if (_isJoining) {
            joinTheChordRing();
        } else {
            _succ = _myName;
            _pred = _myName;
            _isConnected = true;
        }

        while (_isConnected) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.err.println("We were interrupted!");
                e.printStackTrace();
            }
        }

        leaveTheChordRing();

        try {
            server.join();
            client.join();
            messenger.join();
        } catch (InterruptedException e) {

        }

        System.out.println("I have now left the chord ring!");
    }

    private void joinTheChordRing() {
        //        Message getSuccessor = new Message(Message.JOIN, _myKey, getChordName(), getChordName(),
        //                _connectedAt, null);
        //        MessageHandler succHandler = new MessageHandler();
        //        _responseHandlers.put(getSuccessor.ID, succHandler);
        //        _outgoingMessages.add(getSuccessor);
        //        try {
        //            setSuccessor((InetSocketAddress)succHandler.getMessage().payload);
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }
        //
        //        Message getPredecessor = new Message(Message.GET_PREDECESSOR, _myKey,
        //                getChordName(), getChordName(), succ(), null);
        //        MessageHandler getPredHandler = new MessageHandler();
        //        _responseHandlers.put(getPredecessor.ID, getPredHandler);
        //        _outgoingMessages.add(getPredecessor);
        //        try {
        //            setPredecessor((InetSocketAddress)getPredHandler.getMessage().payload);
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }
        //
        //        Message setSuccessor = new Message(Message.SET_SUCCESSOR, _myKey,
        //                getChordName(), getChordName(), pred(), _myName);
        //        Message setPredecessor = new Message(Message.SET_PREDECESSOR, _myKey,
        //                getChordName(), getChordName(), succ(), _myName);
        //        Message migrate = new Message(Message.MIGRATE, _myKey, getChordName(),
        //                getChordName(), succ(), pred());
        //        _outgoingMessages.add(setSuccessor);
        //        _outgoingMessages.add(setPredecessor);
        //        _outgoingMessages.add(migrate);
        while (!_isConnected) {
            Message lookupSuccessor = new Message(Message.JOIN, _myKey, getChordName(),
                    getChordName(), _connectedAt, null);
            MessageHandler lookupSuccessorHandler = new MessageHandler();
            _responseHandlers.put(lookupSuccessor.ID, lookupSuccessorHandler);
            _outgoingMessages.add(lookupSuccessor);
            InetSocketAddress tmpSuccessor = (InetSocketAddress) lookupSuccessorHandler.getMessage().payload;
            System.out.println("tmp successor: " + tmpSuccessor);
            //At this point we have locked our successor and saved his address temporarily.

            Message getPredecessor = new Message(Message.GET_PREDECESSOR, _myKey, getChordName(), getChordName(), tmpSuccessor, null);
            MessageHandler getPredeccessorHandler = new MessageHandler();
            _responseHandlers.put(getPredecessor.ID, getPredeccessorHandler);
            System.out.println(_responseHandlers);
            _outgoingMessages.add(getPredecessor);
            InetSocketAddress tmpPredecessor = (InetSocketAddress) getPredeccessorHandler.getMessage().payload;
            System.out.println("tmpPredecessor: " + tmpPredecessor);
            //At this point we need to make sure that we can take lock on tmpPredecessor.
            Message lockPredecessor = new Message(Message.LOCK, _myKey, getChordName(), getChordName(), tmpPredecessor, null);
            MessageHandler lockPredecessorHandler = new MessageHandler();
            _responseHandlers.put(lockPredecessor.ID, lockPredecessorHandler);
            Boolean OK = (Boolean) lockPredecessorHandler.getMessage().payload;
            System.out.println(OK);
            if (OK) {
                setSuccessor(tmpSuccessor);
                setPredecessor(tmpPredecessor);
                Message setSucc = new Message(Message.SET_SUCCESSOR, _myKey, getChordName(), getChordName(), pred(), getChordName());
                Message setPred = new Message(Message.SET_PREDECESSOR, _myKey, getChordName(), getChordName(), succ(), getChordName());
                Message unlockSucc = new Message(Message.UNLOCK, _myKey, getChordName(), getChordName(), succ(), null);
                Message unlockPred = new Message(Message.UNLOCK, _myKey, getChordName(), getChordName(), pred(), null);
                _outgoingMessages.add(setSucc);
                _outgoingMessages.add(setPred);
                _outgoingMessages.add(unlockPred);
                _outgoingMessages.add(unlockSucc);
                
                //leave loop
                _isConnected = true;
                continue;
            } else {
                //we couldn't require locks, so we must unlock tmp successor and start over
                Message unlockSucc = new Message(Message.UNLOCK, _myKey, getChordName(), getChordName(), tmpSuccessor, null);
                _outgoingMessages.add(unlockSucc);
                try {
                    Thread.sleep(200*new Random().nextInt(5)+1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    private void leaveTheChordRing() {
        Message msg1 = new Message(Message.SET_PREDECESSOR, _myKey, getChordName(),
                getChordName(), succ(), pred());
        Message msg2 = new Message(Message.SET_SUCCESSOR, _myKey, getChordName(),
                getChordName(), pred(), succ());
        _outgoingMessages.add(msg1);
        _outgoingMessages.add(msg2);

        for (String s : _localStore.keySet()) {
            put(s, _localStore.get(s));
        }

        _chordServer.stopServer();
        _chordClient.stopClient();
        //lets wait until all messages are sent!
        while (!_outgoingMessages.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        _chordMessenger.stopSender();
    }

    public String getGraphViz() {
        String result = "";
        result += ChordHelpers.keyOfObject(getChordName()) + " -> " 
                + ChordHelpers.keyOfObject(succ()) + "[label=\"succ\"; fontsize=\"6\"];\n";
        result += ChordHelpers.keyOfObject(getChordName()) 
                + " -> " + ChordHelpers.keyOfObject(pred()) + "[label=\"pred\"; fontsize=\"6\"];\n";
        return result;
    }

    public void migrate(int lowKey, int highKey) {
        Map<String, Object> local = 
                Collections.synchronizedMap(new HashMap<String ,Object>());
        Map<String, Object> out = 
                Collections.synchronizedMap(new HashMap<String ,Object>());

        for (String s : _localStore.keySet()) {
            int key = ChordHelpers.keyOfObject(s);
            if (ChordHelpers.inBetween(lowKey, highKey, key)) {
                out.put(s, _localStore.get(s));
            } else {
                local.put(s, _localStore.get(s));
            }
        }

        _localStore = local;

        for (String s : out.keySet()) {
            put(s, out.get(s));
        }
    }

}
















