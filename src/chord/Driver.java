package chord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

public class Driver {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		interfaces.ChordObjectStorage c;

		String command = "";

		System.out.println("O Captain! my Captain! rise up and hear the bells!");

		InputStreamReader converter = new InputStreamReader(System.in);
		BufferedReader in = new BufferedReader(converter);

		c = new ChordObjectStorageImpl(-1);

		while (!(command.equals("quit"))){
			command = in.readLine();

			if (command.equals("create")) {
				System.out.println("You have asked to create a group, please choose a port: ");
				int port = Integer.parseInt(in.readLine());
				c.createGroup(port);
				System.out.println("Startet a group on: " + c.getChordName());
			} else if (command.equals("join")) {
				System.out.println("You have asked to join a group, please choose a peer on which to join: ");
				command = in.readLine();
				System.out.println("Please choose a port to listen on:");
				int port = Integer.parseInt(in.readLine());
				InetSocketAddress knownPeer = new InetSocketAddress(command.substring(0,command.indexOf(":")), Integer.parseInt(command.substring(command.indexOf(":") + 1)));
				c.joinGroup(knownPeer, port);
				System.out.println("Joined the ring, and my name is: " + c.getChordName());
			} else if (command.equals("put")) {
				System.out.println("You have asked to put a object in the ring. Choose a number: ");
				command = in.readLine();
				int j = Integer.parseInt(command);
				String name = "Name " + j;
				Object object = Integer.valueOf(j);
				System.out.println("Putting " + j);
				c.put(name, object);
			} else if (command.equals("get")) {
				System.out.println("You have asked to find a object in the ring. Choose a number: ");
				command = in.readLine();
				int j = Integer.parseInt(command);
				String name = "Name " + j;
				System.out.println("Getting " + j);
				Object object = c.get(name);
				boolean OK = true;
				if (object==null) {
					System.err.println("ERROR: " + j + " no object!");
					OK = false;
				} else {
					if (!(object instanceof Integer)) {
						System.err.println("ERROR: " + j + " wrong type!");
						OK = false;
					} else {
						if (((Integer)object).intValue()!=j) {
							System.err.println("ERROR: " + j + " wrong value!");
							OK = false;
						}
					}
				}
				if (OK)
					System.out.println("Yay! Got object.");
			} else if (command.equals("leave")) {
				c.leaveGroup();
				System.out.println("Elvis has left the building.");
			}

		}

	}

}
