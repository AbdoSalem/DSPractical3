package de.uni_stuttgart.ipvs.ids.replication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;

import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class Replica<T> extends Thread {

	public enum LockType {
		UNLOCKED, READLOCK, WRITELOCK
	};

	private int id;

	private double availability;
	private VersionedValue<T> value;

	protected DatagramSocket socket = null;
	
	protected LockType lock;
	
	/**
	 * This address holds the addres of the client holding the lock. This
	 * variable should be set to NULL every time the lock is set to UNLOCKED.
	 */
	protected SocketAddress lockHolder;

	public Replica(int id, int listenPort, double availability, T initialValue) throws SocketException {
		super("Replica:" + listenPort);
		this.id = id;
		SocketAddress socketAddress = new InetSocketAddress("127.0.0.1", listenPort);
		this.socket = new DatagramSocket(socketAddress);
		this.availability = availability;
		this.value = new VersionedValue<T>(0, initialValue);
		this.lock = LockType.UNLOCKED;
	}
	

	/**
	 * Part a) Implement this run method to receive and process request
	 * messages. To simulate a replica that is sometimes unavailable, it should
	 * randomly discard requests as long as it is not locked.
	 * The probability for discarding a request is (1 - availability).
	 * 
	 * For each request received, it must also be checked whether the request is valid.
	 * For example:
	 * - Does the requesting client hold the correct lock?
	 * - Is the replica unlocked when a new lock is requested?
	 */
	public void run() {
		// TODO: Implement me!
	}
	
	/**
	 * This is a helper method. You can implement it if you want to use it or just ignore it.
	 * Its purpose is to send a Vote (YES/NO depending on the state) to the given address.
	 */
	protected void sendVote(SocketAddress address,
			Vote.State state, int version) throws IOException {
		// TODO: Implement me!
	}

	/**
	 * This is a helper method. You can implement it if you want to use it or just ignore it.
	 * Its purpose is to extract the object stored in a DatagramPacket.
	 */
	protected Object getObjectFromMessage(DatagramPacket packet)
			throws IOException, ClassNotFoundException {
		// TODO: Implement me!
		return null; // Pacify the compiler
	}

	public int getID() {
		return id;
	}

	public SocketAddress getSocketAddress() {
		return socket.getLocalSocketAddress();
	}

}
