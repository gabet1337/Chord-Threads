package chord;

import interfaces.ResponseHandler;

public class MessageHandler extends Thread implements ResponseHandler {
    
    private Message _message = null;

    public Message getMessage() throws InterruptedException {
        if (_message == null) {
            System.out.println("Waiting for reply!");
            Thread.sleep(1000);
            return getMessage();
        } else {
            return _message;
        }
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
