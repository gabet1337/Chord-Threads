package chord;

import java.net.*;

public class Message {
    
    public static final int JOIN = 1;
    public static final int LOOKUP = 2;
    public static final int SET_PREDECESSOR = 3;
    public static final int SET_SUCCESSOR = 4;
    public static final int GET_PREDECESSOR = 5;
    public static final int GET_SUCCESSOR = 6;
    public static final int GET_OBJECT = 7;
    public static final int SET_OBJECT = 8;
    public static final int RESULT = 9;
    
    public int type;
    
    public InetSocketAddress origin;
    
    public int key;
    
    public Object payload;
    
    public Message(int type) {
        this.type = type;
    }

}
