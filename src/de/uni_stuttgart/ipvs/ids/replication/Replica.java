package de.uni_stuttgart.ipvs.ids.replication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

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
	private int count =0;
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
	 * randomly discard requests as long as it is not locked. The probability
	 * for discarding a request is (1 - availability).
	 * 
	 * For each request received, it must also be checked whether the request is
	 * valid. For example: - Does the requesting client hold the correct lock? -
	 * Is the replica unlocked when a new lock is requested?
	 */
	public void run() {
		// TODO: Implement me!
		while (true) {
			byte[] bytes = new byte[1024];
			DatagramPacket dummyPacket = new DatagramPacket(bytes, bytes.length);
			try {
				socket.receive(dummyPacket);
				Object request = getObjectFromMessage(dummyPacket);
				SocketAddress address = dummyPacket.getSocketAddress();
				
				if (request instanceof ReleaseReadLock) {
					//Received a release read lock message
					if (lock == LockType.READLOCK && lockHolder.equals(address)) {
						//if the current lock is read and the sender is the lockholder 
						//Then change lock to unlock remove the lockholder and send an ACK	
						lock = LockType.UNLOCKED;
						lockHolder = null;
						sendVote(address, Vote.State.YES, -1);
					} else {
						//else send an ACK with NO 
						sendVote(address, Vote.State.NO, -1);
					}
				} else if (request instanceof ReleaseWriteLock) {
					//Received a release write lock message
					if (lock == LockType.WRITELOCK && lockHolder.equals(address)) {
						//if the current lock is write and the sender is the lockholder 
						//Then change lock to unlock remove the lockholder and send an ACK
						lock = LockType.UNLOCKED;
						lockHolder = null;
						sendVote(address, Vote.State.YES, -1);
					} else {
						//else send An ACK with no 
						sendVote(address, Vote.State.NO, -1);
					}
				} else if (request instanceof RequestReadVote || request instanceof RequestWriteVote) {
					//Received a vote request message
					//increment no. of received requests
					count++;
					if(count >9)
						count =0;
					//if the current count is above availability then discard
					if(count > availability*10)
						continue;
					if (lock == LockType.UNLOCKED) {
						//if the current state is unlocked then send a YES vote with the current version 
						sendVote(address, Vote.State.YES, value.getVersion());
						//set a lock with either read or write depending on type
						lock = request instanceof RequestWriteVote? LockType.WRITELOCK: LockType.READLOCK;
						// set lockholder
						lockHolder = new InetSocketAddress(dummyPacket.getAddress(),dummyPacket.getPort()); 
						
					} else {
						//else send a NO vote with the current version
						sendVote(address, Vote.State.NO, value.getVersion());
					}
//				} else if (request instanceof RequestWriteVote) {
//					//Received a request write vote message
//					if(count > availability*10)
//						continue;
//					if (lock == LockType.UNLOCKED /*
//													 * || (lock ==
//													 * LockType.READLOCK &&
//													 * lockHolder.equals(address
//													 * ))
//													 */) {
//						//if the current state is unlocked then send a YES vote with the current version
//						sendVote(address, Vote.State.YES, value.getVersion());
//					} else {
//						//else send a NO vote with the current version
//						sendVote(address, Vote.State.NO, value.getVersion());
//					}
				} else if (request instanceof ReadRequestMessage) {
					//Received a read request message
					if (lockHolder != null && lock != LockType.UNLOCKED && lockHolder.equals(address)) {
						//if the lock is either read or write and the lock holder is the sender of the message 
						//Then create a response value message with the current value and send it
						synchronized (value) {
							ValueResponseMessage<T> response = new ValueResponseMessage<T>(value.getValue());
							sendPacket(response, address);
						}
					}
				} else if (request instanceof WriteRequestMessage<?>) {
					//Received a write request message
					if (lockHolder != null && lock == LockType.WRITELOCK && lockHolder.equals(address)) {
						//if the lock is write and the lock holder is the sender of the message 
						//Then create a new versioned value from the request message and replace the current value and send a YES vote as ACK
						WriteRequestMessage<T> writeRequest = (WriteRequestMessage<T>) request;
						if (writeRequest.version > value.getVersion()) {
							synchronized (value) {
								value = new VersionedValue<T>(writeRequest.getVersion(), writeRequest.getValue());
								sendVote(address, Vote.State.YES, -1);
							}

						}else{
							//else send an ACK with no
							sendVote(address, Vote.State.NO, value.getVersion());
						}
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This is a helper method. You can implement it if you want to use it or
	 * just ignore it. Its purpose is to send a Vote (YES/NO depending on the
	 * state) to the given address.
	 */
	protected void sendVote(SocketAddress address, Vote.State state, int version) throws IOException {
		// TODO: Implement me!

		// create a vote with the passes state and version
		Vote vote = new Vote(state, version);
		// send message
		sendPacket(vote, address);
	}

	/**
	 * This is a helper method. You can implement it if you want to use it or
	 * just ignore it. Its purpose is to extract the object stored in a
	 * DatagramPacket.
	 */
	protected Object getObjectFromMessage(DatagramPacket packet) throws IOException {
		// TODO: Implement me!
		try {
			// create inout streams to deserialize byte[] to object
			ByteArrayInputStream b = new ByteArrayInputStream(packet.getData());
			ObjectInputStream o = new ObjectInputStream(b);
			return o.readObject();

		} catch (Exception e) {
			System.out.println("Exception occured while reading packet");
			e.printStackTrace();
			return null; // Pacify the compiler
		}

	}

	/*
	 * This method creates a datagram packet with the to send object and send it
	 * to the input address
	 */
	private void sendPacket(Object toSend, SocketAddress address) {
		byte[] data;
		try {
			// convert object to byte[]
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			ObjectOutputStream o = new ObjectOutputStream(b);
			o.writeObject(toSend);
			data = b.toByteArray();
			// create the data packet and pass the byte[] to it and set
			// address to passed address
			DatagramPacket packet = new DatagramPacket(data, data.length);
			packet.setSocketAddress(address);
			// send packet
			socket.send(packet);
		} catch (Exception e) {
			System.out.println("Exception occured while sending packet");
			e.printStackTrace();
		}
	}
	
	public int getID() {
		return id;
	}

	public SocketAddress getSocketAddress() {
		return socket.getLocalSocketAddress();
	}
	
}
