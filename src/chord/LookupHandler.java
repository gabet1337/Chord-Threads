package chord;

import interfaces.ResponseHandler;

public class LookupHandler implements ResponseHandler {
    
    private Message _message = null;

    public Message getMessage() throws InterruptedException {
        if (_message == null) {
            wait();
            return getMessage();
        } else {
            return _message;
        }
    }

    public void setMessage(Message message) {
        _message = message;
        notify();
    }

}
