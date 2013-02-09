package chord;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import services.*;

public class ChordServer implements Runnable {

    private BlockingQueue<Message> _incomingMessages;
    private ServerSocket _serverSocket;
    
    private boolean _isRunning;

    public ChordServer(BlockingQueue<Message> incoming, int port) {
        _incomingMessages = incoming;
        _serverSocket = ChordHelpers.getServerSocket(port);
        _isRunning = true;
    }

    public void run() {
        while (_isRunning) {
            Socket socket = waitForConnection();
            Message msg = receiveMessage(socket);
            _incomingMessages.add(msg);
            closeConnection(socket);
            Thread.yield();
        }
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
    }

}