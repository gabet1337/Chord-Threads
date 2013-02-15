package chord;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import services.*;
import interfaces.*;

public class ChordObjectStorageImpl extends DDistThread implements ChordObjectStorage {

    private boolean _isJoining;
    public volatile boolean _isLeaving = false;
    public volatile boolean _isConnected = false;
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

    public volatile BlockingQueue<Message> _incomingMessages = new LinkedBlockingQueue<Message>();
    public volatile BlockingQueue<Message> _outgoingMessages = new LinkedBlockingQueue<Message>();

    public volatile Map<Integer, ResponseHandler> _responseHandlers = 
            Collections.synchronizedMap(new HashMap<Integer ,ResponseHandler>());

    public volatile Map<String, Object> _localStore = 
            Collections.synchronizedMap(new HashMap<String ,Object>());

    public ChordObjectStorageImpl(int expectedLifeSpan) {
        super(expectedLifeSpan);
        _isConnected = false;
        _wasConnected = false;
    }

    public boolean getIsJoining() {
        return _isJoining;
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
        init();
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

        init();
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
        debug("Shutting down!");
        synchronized(_connectedLock) {
        	_isLeaving = true;
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
            _chordClient.lock();
        } else {
            //forward to successor
            message.receiver = succ();
            message.sender = getChordName();
        }
        enqueueMessage(message);
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
        enqueueMessage(message);
        return (InetSocketAddress) handler.getMessage().payload;
    }

    public void debug(String message) {
    	//if (getChordName().getPort() == 40000)
    	//	System.out.println("" + getChordName().getPort() +  ": " + message);
    }

    public void put(String name, Object object) {
    	
    	 while (!_isConnected && !_isLeaving) {
    		 debug("Will no do that until i am connected");
    	 }
    	
        int key = ChordHelpers.keyOfObject(name);
        Message message = new Message(Message.SET_OBJECT, key, getChordName(),
                getChordName(), succ(), object);
        message.name = name;
        debug("put " + object.toString() + " in outgoing queue.");
        enqueueMessage(message);
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
        enqueueMessage(message);
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

    private Thread _t_server;
    private Thread _t_client;
    private Thread _t_messenger;
    
    public void init() {

        _chordServer = new ChordServer(_incomingMessages, _port, this);
        _chordClient = new ChordClient(this);
        _chordMessenger = new ChordMessageSender(_outgoingMessages, this);
        
        if (_isJoining) _chordClient.lock();

        _t_server = new Thread(_chordServer);
        _t_client = new Thread(_chordClient);
        _t_messenger = new Thread(_chordMessenger);
        
    }

    public void run() {
    	debug("Starting");

        _t_server.start();
        _t_client.start();
        _t_messenger.start();

        if (_isJoining) {
            joinTheChordRing();
        } else {
            _succ = _myName;
            _pred = _myName;
            _isConnected = true;
        }

        _chordClient.unlock();

        while (_isConnected) {
            try {
                Thread.sleep(1000);
                //debug("_isConnected is up. My localStore is " + _localStore.toString());
            } catch (InterruptedException e) {
                System.err.println("We were interrupted!");
                e.printStackTrace();
            }
        }

        debug("_isConnected just went down. My localStore is " + _localStore.toString());

        leaveTheChordRing();

        try {
            _t_server.join();
            _t_client.join();
            _t_messenger.join();
        } catch (InterruptedException e) {

        }
        debug("I have now left the chord ring!");
    }

    private void joinTheChordRing() {
        debug("joining");
        //        Message getSuccessor = new Message(Message.JOIN, _myKey, getChordName(), getChordName(),
        //                _connectedAt, null);
        //        MessageHandler succHandler = new MessageHandler();
        //        _responseHandlers.put(getSuccessor.ID, succHandler);
        //        enqueueMessage(getSuccessor);
        //        setSuccessor((InetSocketAddress)succHandler.getMessage().payload);
        //
        //        Message getPredecessor = new Message(Message.GET_PREDECESSOR, _myKey,
        //                getChordName(), getChordName(), succ(), null);
        //        MessageHandler getPredHandler = new MessageHandler();
        //        _responseHandlers.put(getPredecessor.ID, getPredHandler);
        //        enqueueMessage(getPredecessor);
        //        setPredecessor((InetSocketAddress)getPredHandler.getMessage().payload);
        //
        //        Message setSuccessor = new Message(Message.SET_SUCCESSOR, _myKey,
        //                getChordName(), getChordName(), pred(), _myName);
        //        Message setPredecessor = new Message(Message.SET_PREDECESSOR, _myKey,
        //                getChordName(), getChordName(), succ(), _myName);
        //        Message migrate = new Message(Message.MIGRATE, _myKey, getChordName(),
        //                getChordName(), succ(), pred());
        //        enqueueMessage(setSuccessor);
        //        enqueueMessage(setPredecessor);
        //        enqueueMessage(migrate);
        while (!_isConnected) {
            Message lookupSuccessor = new Message(Message.JOIN, _myKey, getChordName(),
                    getChordName(), _connectedAt, null);
            MessageHandler lookupSuccessorHandler = new MessageHandler();
            _responseHandlers.put(lookupSuccessor.ID, lookupSuccessorHandler);
            enqueueMessage(lookupSuccessor);
            InetSocketAddress tmpSuccessor = (InetSocketAddress) lookupSuccessorHandler.getMessage().payload;
            //At this point we have locked our successor and saved his address temporarily.

            Message getPredecessor = new Message(Message.GET_PREDECESSOR, _myKey, getChordName(), getChordName(), tmpSuccessor, null);
            MessageHandler getPredeccessorHandler = new MessageHandler();
            _responseHandlers.put(getPredecessor.ID, getPredeccessorHandler);
            enqueueMessage(getPredecessor);
            InetSocketAddress tmpPredecessor = (InetSocketAddress) getPredeccessorHandler.getMessage().payload;
            //At this point we need to make sure that we can take lock on tmpPredecessor.
            Boolean OK = true;
            if (!tmpPredecessor.equals(tmpSuccessor)) {
                //we are the first node to join the creator and he is therefore locked already
                Message lockPredecessor = new Message(Message.LOCK, _myKey, getChordName(), getChordName(), tmpPredecessor, null);
                MessageHandler lockPredecessorHandler = new MessageHandler();
                _responseHandlers.put(lockPredecessor.ID, lockPredecessorHandler);
                enqueueMessage(lockPredecessor);
                OK = (Boolean) lockPredecessorHandler.getMessage().payload;
            }

            if (OK) {
                setSuccessor(tmpSuccessor);
                setPredecessor(tmpPredecessor);
                Message setSucc = new Message(Message.SET_SUCCESSOR, _myKey, getChordName(), getChordName(), pred(), getChordName());
                Message setPred = new Message(Message.SET_PREDECESSOR, _myKey, getChordName(), getChordName(), succ(), getChordName());
                Message unlockSucc = new Message(Message.UNLOCK, _myKey, getChordName(), getChordName(), succ(), null);
                Message unlockPred = new Message(Message.UNLOCK, _myKey, getChordName(), getChordName(), pred(), null);
                Message migrate = new Message(Message.MIGRATE, _myKey, getChordName(), getChordName(), succ(), pred());
                enqueueMessage(setSucc);
                enqueueMessage(setPred);
                enqueueMessage(unlockPred);
                enqueueMessage(unlockSucc);
                enqueueMessage(migrate);

                //leave loop
                _isConnected = true;
                continue;
            } else {
                //we couldn't require locks, so we must unlock tmp successor and start over
                Message unlockSucc = new Message(Message.UNLOCK, _myKey, getChordName(), getChordName(), tmpSuccessor, null);
                enqueueMessage(unlockSucc);
                try {
                    Thread.sleep(200*new Random().nextInt(5)+1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void leaveTheChordRing() {

        debug("leaving the chord ring with the following localStore: " + _localStore.toString());

        Message msg1 = new Message(Message.SET_PREDECESSOR, _myKey, getChordName(),
                getChordName(), succ(), pred());
        Message msg2 = new Message(Message.SET_SUCCESSOR, _myKey, getChordName(),
                getChordName(), pred(), succ());
        enqueueMessage(msg1);
        enqueueMessage(msg2);

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
        result += ChordHelpers.keyOfObject(getChordName()) / 1000000 + " -> " 
                + ChordHelpers.keyOfObject(succ()) / 1000000 + "[label=\"succ\"; fontsize=\"6\"];\n";
        result += ChordHelpers.keyOfObject(getChordName()) / 1000000
                + " -> " + ChordHelpers.keyOfObject(pred()) / 1000000 + "[label=\"pred\"; fontsize=\"6\"];\n";
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

    public void enqueueMessage(Message message) {
        synchronized (_outgoingMessages) {
            _outgoingMessages.add(message);
            _outgoingMessages.notify();
        }
    }

}
















