package chord;
import java.io.IOException;
import java.net.*;
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

    private Object _connectedLock = new Object();

    private ServerSocket _serverSocket;

    private BlockingQueue<Message> _incomingMessages = new LinkedBlockingQueue<Message>();


    public ChordObjectStorageImpl(int expectedLifeSpan) {
        super(expectedLifeSpan);
        _isConnected = false;
        _wasConnected = false;
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
        synchronized(_connectedLock) {
            _isConnected = false;
        }
    }

    public InetSocketAddress succ() {
        return _succ;
    }

    public InetSocketAddress pred() {
        return _pred;
    }

    public InetSocketAddress lookup(int key) {
        return null;
    }

    public void put(String name, Object object) {

    }

    public Object get(String name) {
        return null;
    }

    public void run() {

        if (_isJoining) {
            //join the chord
        } else {
            //create the chord
        }

        ChordServer chordServer = new ChordServer(_incomingMessages, _port);
        ChordClient chordClient = new ChordClient(_incomingMessages, this);

        Thread server = new Thread(chordServer);
        Thread client = new Thread(chordClient);

        server.start();
        client.start();
        
        while (_isConnected) {
            yield();
        }
        chordServer.stopServer();
        chordClient.stopClient();

    }

}
















