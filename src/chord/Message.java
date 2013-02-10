package chord;

import java.io.Serializable;
import java.net.*;
import java.util.Random;

public class Message implements Serializable {

    private static final long serialVersionUID = 6244711900508423510L;

    public static final int JOIN = 1;
    public static final int LOOKUP = 2;
    public static final int SET_PREDECESSOR = 3;
    public static final int SET_SUCCESSOR = 4;
    public static final int GET_PREDECESSOR = 5;
    public static final int GET_SUCCESSOR = 6;
    public static final int GET_OBJECT = 7;
    public static final int SET_OBJECT = 8;
    public static final int RESULT = 9;

    public int ID = new Random().nextInt();

    public int type;

    public InetSocketAddress origin;

    public InetSocketAddress sender;

    public InetSocketAddress receiver;

    public int key;

    public Object payload;
    
    public String name;

    public Message(int type, int key, InetSocketAddress origin, InetSocketAddress sender,
            InetSocketAddress receiver, Object payload) {
        this.type = type;
        this.key = key;
        this.origin = origin;
        this.sender = sender;
        this.receiver = receiver;
        this.payload = payload;
    }

    public String toString() {
        String result = 
                "ID: " + ID + "\n" +
                "ORIGIN: " + origin +  "\n" +
                "SENDER: " + sender + "\n" +
                "RECEIVER: " + receiver + "\n" +
                "TYPE: " + getTypeString() + "\n" +
                "KEY: " + key + "\n" +
                "PAYLOAD: " + payload + "\n";
        return result;
    }

    private String getTypeString() {
        switch (type) {
        case JOIN : return "JOIN";
        case LOOKUP : return "LOOKUP";
        case SET_PREDECESSOR : return "SET_PREDECESSOR";
        case SET_SUCCESSOR : return "SET_SUCCESSOR";
        case GET_PREDECESSOR : return "GET_PREDECESSOR";
        case GET_SUCCESSOR : return "GET_SUCCESSOR";
        case GET_OBJECT : return "GET_OBJECT";
        case RESULT : return "RESULT";
        default: return "ERROR";
        }
    }

}
