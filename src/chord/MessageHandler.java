package chord;

import interfaces.ResponseHandler;

public class MessageHandler extends Thread implements ResponseHandler {
    
    private Message _message = null;

    public Message getMessage() throws InterruptedException {
        while (_message == null) { Thread.yield();}
        return _message;
    }

    public void setMessage(Message message) {
        _message = message;
    }
    
    public void run() {
        try {
            getMessage();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
