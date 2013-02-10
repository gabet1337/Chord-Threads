package interfaces;

import chord.Message;

public interface ResponseHandler {
    
    /**
     * When the message is ready we can call this one
     * @throws InterruptedException 
     */
    public Message getMessage() throws InterruptedException;
    
    /**
     * Tells this handler his message is ready.
     * @param message His new message
     */
    public void setMessage(Message message);

}
