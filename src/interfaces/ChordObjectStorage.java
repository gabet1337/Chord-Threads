package interfaces;
import java.net.InetSocketAddress;

/**
 *
 * Interface for a Chord file service. Each peer is named by an IP
 * address and a port, technically an InetSocketAddress. Each
 * InetSocketAddress is mapped into a key, an unsigned 31-bit integer,
 * by taking H=InetSocketAddress.hashCode() and using a cryptographic
 * hash function to hash H, as to give a deterministic but random
 * looking result. The same key can be computed from any Object, using
 * .hashCode(). The key is used to arrange all InetSocketAddress's and
 * Objects into a ring, with the current peers being responsible for
 * each their interval of the key space for the Objects, according to
 * the Chord network topology. The interface allows to enter and leave
 * a chord group and allows to find the name of a peer currently
 * responsible for a given key. It also allows to store an object in
 * the object storage service and to get an object back.
 */

public interface ChordObjectStorage {

    /**
     * Used by the first group member.  Specifies the port on which
     * this founding peer is listening for new peers to join or leave.
     * The name of the foudning peer is its local IP address and the
     * given port. Its key is derived from the name using the method
     * described above.
     *
     * May only be called once!
     *
     * @param port The port number on which this founding peer is listening.
     *
     */
    public void createGroup(int port);

    /**
     * Used to join a Chord group. This takes place by contacting one
     * of the existing peers of the Chord group.  The new peer has the
     * name specified by the local IP address and the given port. The
     * key of the new peer is derived from its name using the method
     * described above.
     *      
     * May only be called once!
     *
     * @param port The port number on which the new peer is waiting for peers.
     * @param serverAddress The IP address and port of the known peer.
     */
    public void joinGroup(InetSocketAddress knownPeer, int port);

    /**
     * Reports if this peer is connected to a peer group. There might
     * be an interval of time between a call to createGroup or
     * joinGroup until the the peer is actually connected.
     */
    public boolean isConnected();

    /**
     * Returns the name of this peer. May only be called after a group
     * has been formed or joined.
     */
    public InetSocketAddress getChordName();

    /**
     * Makes this instance of ChordNameService leave the peer
     * group. The other peers should be informed of this and the Chord
     * network updated appropriately.
     */
    public void leaveGroup();
    
    /**
     * Returns the current successor of this peer. In a singleton
     * group, the successor is this instance itself.
     */
    public InetSocketAddress succ();

    /**
     * Returns the current predecessor of this peer. In a
     * singleton group, the predecessor is this instance itself.
     */
    public InetSocketAddress pred();

    /**
     * Returns the name of the peer who is currently responsible
     * for a given key, according to the topology of the Chord
     * network.
     *
     * @param key The key for which we seek the responsible peer. Must be non-negative.
     */
    public InetSocketAddress lookup(int key);

    /**
     * Stores the object in the Chord network. The call should be
     * asynchronous in that it returns to the caller immediately, as
     * opposed to waiting until the object has been properly stored in
     * the Chord network. This allows to conviniently store a lot of
     * objects in parallel.
     */
    public void put(String name, Object object);

    /**
     * Retrieves the object from the Chord network. The call is
     * synchronous.
     */
    public Object get(String name);

}
