package chord;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import services.*;

public class ChordServer implements Runnable {

    private BlockingQueue<Message> _incomingMessages;
    private ServerSocket _serverSocket;

    private boolean _isRunning;
    
    private ChordObjectStorageImpl _nodeReference;

    public ChordServer(BlockingQueue<Message> incoming, int port, ChordObjectStorageImpl nodeReference) {
        _incomingMessages = incoming;
        _serverSocket = ChordHelpers.getServerSocket(port);
        _isRunning = true;
        _nodeReference = nodeReference;
    }

    public void run() {
        while (_isRunning) {
            //            System.out.println("ID: " + _serverSocket + " waiting for connection");
            Socket socket = waitForConnection();
            if (socket != null) {
                Message msg = receiveMessage(socket);
                _incomingMessages.add(msg);
                closeConnection(socket);
                Thread.yield();
            }
        }
        _nodeReference.debug("Server stopped");
    }

    private Message receiveMessage(Socket socket) {
        Message result = null;
        try {
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            result = (Message) ois.readObject();
        } catch (SocketTimeoutException e) {
            System.err.println("Connection timed out");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return result;
    }

    private Socket waitForConnection() {
        Socket result = null;
        try {
            result = _serverSocket.accept();
        } catch (SocketException e) {
        	_nodeReference.debug("We were forcefully closed!");
            System.err.println("We were forcefully closed!");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private void closeConnection(Socket socket) {
        try {
            socket.close();
        } catch (SocketException e) {
            System.err.println("Blocked on I/O");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stopServer() {
        _isRunning = false;
        _nodeReference.debug("Stopping server");
        try {
            _serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
