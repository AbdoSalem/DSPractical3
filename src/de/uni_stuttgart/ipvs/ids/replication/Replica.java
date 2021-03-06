package de.uni_stuttgart.ipvs.ids.replication;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Random;
import de.uni_stuttgart.ipvs.ids.communication.PacketModel;
import de.uni_stuttgart.ipvs.ids.communication.PacketSendReceive;
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
	private final int DUTY_CYCLE=100;
	protected LockType lock;
	private int count =0;
	/**
	 * This address holds the addres of the client holding the lock. This
	 * variable should be set to NULL every time the lock is set to UNLOCKED.
	 */
	protected SocketAddress lockHolder;
	private Random rand = new Random();
	
	public Replica(int id, int listenPort, double availability, T initialValue) throws SocketException {
		super("Replica:" + listenPort);
		this.id = id;
		SocketAddress socketAddress = new InetSocketAddress("127.0.0.1", listenPort);
		this.socket = new DatagramSocket(socketAddress);
		this.availability = availability;
		this.value = new VersionedValue<T>(0, initialValue);
		this.lock = LockType.UNLOCKED;
		log("new Replica: "+toString() );		
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
		
			try {
				PacketModel model = PacketSendReceive.receivePacket(socket);
				Object request = model.getData();
				SocketAddress address = model.getAddress();
				if(lock == LockType.UNLOCKED){
					//====================dutycycle=====================
					count++;
					if(count >DUTY_CYCLE-1)
							count =0;
					log("Count is "+ count);
					//=================================================
					int n=rand.nextInt(DUTY_CYCLE)+1;
					log("the random number is "+n+" availability is " +(availability*DUTY_CYCLE));
					//if the current count is above availability then discard
					//For a random failure
					if(n > (availability*DUTY_CYCLE)){ 
					// ============if a dutycycle is required here is its implementation===============
//					if(count >= availability*DUTY_CYCLE){
						log("Dropping message");
						continue;
					}
				}
				if (request instanceof ReleaseReadLock) {
					//Received a release read lock message
					log("Release Read lock Message");
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
					log("Release Write lock Message");
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

					
					
					log("Request Read vote or write vote");
					if (lock == LockType.UNLOCKED) {
						//if the current state is unlocked then send a YES vote with the current version 
						sendVote(address, Vote.State.YES, value.getVersion());
						//set a lock with either read or write depending on type
						lock = request instanceof RequestWriteVote? LockType.WRITELOCK: LockType.READLOCK;
						log("State changed to "+ (request instanceof RequestWriteVote? "WRITELOCK": "READLOCK"));
						// set lockholder
						lockHolder = address; 
						log("Voting yes to "+ ((InetSocketAddress)address).getHostName()+":"+((InetSocketAddress)address).getPort());	
					} else {
						//else send a NO vote with the current version
						log("Voting No As state is "+ lock);
						sendVote(address, Vote.State.NO, value.getVersion());
					}
//					***********************This part is for upgrading the lock if it is supported********************
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
//					******************************************************************************************
				} else if (request instanceof ReadRequestMessage) {
					//Received a read request message
					log("it is a read request Message");
					if (lockHolder != null && lock != LockType.UNLOCKED && lockHolder.equals(address)) {
						//if the lock is either read or write and the lock holder is the sender of the message 
						//Then create a response value message with the current value and send it
						synchronized (value) {
							log("Returning value with version"+ value.toString() );
							ValueResponseMessage<T> response = new ValueResponseMessage<T>(value.getValue());
							PacketSendReceive.sendPacket(socket,response, address);
						}
					}else {						
						sendVote(address, Vote.State.NO, -1);
					}
				} else if (request instanceof WriteRequestMessage<?>) {
					//Received a write request message
					log("it is a wrte request message "+ ((WriteRequestMessage<T>)request).toString());
					if (lockHolder != null && lock == LockType.WRITELOCK && lockHolder.equals(address)) {
						//if the lock is write and the lock holder is the sender of the message 
						//Then create a new versioned value from the request message and replace the current value and send a YES vote as ACK
						WriteRequestMessage<T> writeRequest = (WriteRequestMessage<T>) request;
						log("Attempt to write value "+ value.toString());
						if (writeRequest.version > value.getVersion()) {
							synchronized (value) {
								value = new VersionedValue<T>(writeRequest.getVersion(), writeRequest.getValue());
								sendVote(address, Vote.State.YES, -1);
								log("Attempt successfull");
							}

						}else{
							//else send an ACK with no
							log("Attempt unsuccessfull");
							sendVote(address, Vote.State.NO, value.getVersion());
						}
					}else{
						//else send an ACK with no
						log("Attempt unsuccessfull");
						sendVote(address, Vote.State.NO, value.getVersion());
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
		PacketSendReceive.sendPacket(socket,vote, address);
	}

	/**
	 * This is a helper method. You can implement it if you want to use it or
	 * just ignore it. Its purpose is to extract the object stored in a
	 * DatagramPacket.
	 */
	protected Object getObjectFromMessage(DatagramPacket packet) throws IOException {
		// TODO: Implement me!
		throw new UnsupportedOperationException("Used another function");

	}


	public int getID() {
		return id;
	}

	public SocketAddress getSocketAddress() {
		return socket.getLocalSocketAddress();
	}
	private void log(String s){
		System.out.println("Node("+id+"): "+s);
	}

	@Override
	public String toString() {
		return "Replica [id=" + id + ", availability=" + availability + ", value=" + value + ", socket=" + socket
				+ ", lock=" + lock + ", count=" + count + ", lockHolder=" + lockHolder + "]";
	}
	
	
}
