package chord;

import interfaces.ResponseHandler;

public class MessageHandler implements ResponseHandler {

    private Message _message = null;

    private Object lock = new Object();

    public Message getMessage() {
        while (_message == null) { 
            try {
                synchronized (lock) {
                    lock.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return _message;
    }

    public void setMessage(Message message) {
        _message = message;
        synchronized (lock) {
            lock.notify();
        }
    }
}