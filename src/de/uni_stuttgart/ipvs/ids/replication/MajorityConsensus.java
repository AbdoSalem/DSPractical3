package de.uni_stuttgart.ipvs.ids.replication;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import de.uni_stuttgart.ipvs.ids.communication.MessageWithSource;
import de.uni_stuttgart.ipvs.ids.communication.NonBlockingReceiver;
import de.uni_stuttgart.ipvs.ids.communication.PacketModel;
import de.uni_stuttgart.ipvs.ids.communication.PacketSendReceive;
import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.Vote.State;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class MajorityConsensus<T> {

	protected Collection<SocketAddress> replicas;

	protected DatagramSocket socket;
	protected NonBlockingReceiver nbio;

	final static int TIMEOUT = 1000;
	final static int RETRIES = 5;
	
	public MajorityConsensus(Collection<SocketAddress> replicas)
			throws SocketException {
		this.replicas = replicas;
		SocketAddress address = new InetSocketAddress("127.0.0.1", 4999);
		this.socket = new DatagramSocket(address);
		this.nbio = new NonBlockingReceiver(socket);
	}

	/**
	 * Part c) Implement this method.
	 * send readrequestvote to all replicas if the required number of replicas replied with true then stop
	 * each message has a timeout if that happens just discard the replica and do not add it to the quiriom
	 * if a replica replied with yes add it to the Quorum 
	 * at the end make sure that the Quorumvvvvv is ok
	 */
	protected Collection<MessageWithSource<Vote>> requestReadVote() throws QuorumNotReachedException {
		// TODO: Implement me!

		ArrayList<MessageWithSource<Vote>> quiriom = new ArrayList<MessageWithSource<Vote>>();
		RequestReadVote request = new RequestReadVote();
		for (Iterator<SocketAddress> iterator = replicas.iterator(); iterator.hasNext();) {
			SocketAddress replicaAddress= iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, request, replicaAddress);				
				PacketModel received = PacketSendReceive.receivePacket(socket,TIMEOUT);			
				if(received != null && received.getData() instanceof Vote && received.getAddress().equals(replicaAddress)){
					Vote vote = (Vote) received.getData();
					if(vote.getState() == State.YES)
						quiriom.add(new MessageWithSource<Vote>(replicaAddress, vote));
					if(quiriom.size()> replicas.size()/2)
						break;
				}	
			}catch(TimeoutException e){
				System.out.println("Time out of one replica");
			}
		}
		
		return checkQuorum(quiriom);
		
	}
	
	/**
	 * Part c) Implement this method.
	 * send a ReleaseReadLock msg o all locked replicas again each message has its own timeout
	 * if msg replies remove it from the list of locked replicas so 
	 * if timeout occurs just re-send the msgs to all remaining blocked replicas to make sure all replicas released the locks 
	 */
	protected void releaseReadLock(Collection<SocketAddress> lockedReplicas) {
		// TODO: Implement me!
		ReleaseReadLock msg = new ReleaseReadLock();
		for (Iterator iterator = lockedReplicas.iterator(); iterator.hasNext();) {
			SocketAddress socketAddress = (SocketAddress) iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, msg, socketAddress);
				PacketModel model = PacketSendReceive.receivePacket(socket,TIMEOUT);
				if(model.getData() instanceof Vote){
					iterator.remove();					
				}
			}catch(TimeoutException e){
				System.out.println("Time out on read release lock trying again");
				releaseReadLock(lockedReplicas);
				break;
			}
		}
	}
	
	/**
	 * Part d) Implement this method.
	 *  send RequestWriteVote to all replicas if the required number of replicas replied with true then stop
	 * each message has a timeout if that happens just discard the replica and do not add it to the quiriom
	 * if a replica replied with yes add it to the Quorum
	 * at the end make sure that the Quorum is ok
	 */
	protected Collection<MessageWithSource<Vote>> requestWriteVote() throws QuorumNotReachedException {
		// TODO: Implement me!
		ArrayList<MessageWithSource<Vote>> quiriom = new ArrayList<MessageWithSource<Vote>>();
		RequestWriteVote request = new RequestWriteVote();
		for (Iterator<SocketAddress> iterator = replicas.iterator(); iterator.hasNext();) {
			SocketAddress replicaAddress= iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, request, replicaAddress);				
				PacketModel received = PacketSendReceive.receivePacket(socket,TIMEOUT);			
				if(received!= null&& received.getData() instanceof Vote && received.getAddress().equals(replicaAddress)){
					Vote vote = (Vote) received.getData();
					if(vote.getState() == State.YES)
						quiriom.add(new MessageWithSource<Vote>(replicaAddress, vote));
					if(quiriom.size()> replicas.size()/2)
						break;
				}	
			}catch(TimeoutException e){
				System.out.println("Time out of one replica");				
			}	
		}
		
		return checkQuorum(quiriom);
	}
	
	/**
	 * Part d) Implement this method.
	 * send a ReleaseWriteLock msg o all locked replicas again each message has its own timeout
	 * if msg replies remove it from the list of locked replicas so 
	 * if timeout occurs just re-send the msgs to all remaining blocked replicas to make sure all replicas released the locks 
	 */
	protected void releaseWriteLock(Collection<SocketAddress> lockedReplicas) {
		// TODO: Implement me!
		ReleaseWriteLock msg = new ReleaseWriteLock();
		for (Iterator iterator = lockedReplicas.iterator(); iterator.hasNext();) {
			SocketAddress socketAddress = (SocketAddress) iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, msg, socketAddress);
				System.out.println("sending release");
				PacketModel model = PacketSendReceive.receivePacket(socket,TIMEOUT);
				if(model.getData() instanceof Vote){
					iterator.remove();	
					System.out.println("received ack");
				}
				
			}catch(TimeoutException ex){
				System.out.println("Time out on release write lock trying again");
				releaseWriteLock(lockedReplicas);
				break;
			}
		}
	}
	
	/**
	 * Part c) Implement this method.
	 * send a ReadRequestMessage to the replica and wait for its reply also with a timeout
	 * then parse the response to extract the verisoned value
	 * if not all replicas replied try again for a specific no. of replies else stop and return null 
	 *
	 */
	int count =0;
	protected T readReplica(SocketAddress replica) {
		// TODO: Implement me!
		try{
			ReadRequestMessage msg = new ReadRequestMessage();
			PacketSendReceive.sendPacket(socket, msg, replica);
			PacketModel received = PacketSendReceive.receivePacket(socket,TIMEOUT);	
			
			if(received.getData() instanceof ValueResponseMessage<?> && received.getAddress().equals(replica)){
				ValueResponseMessage<T> response = (ValueResponseMessage<T>) received.getData();
				return response.getValue();				
			}	
			count =0;
		}catch(TimeoutException ex){
			System.out.println("Timeout exception");
			if(count < RETRIES){
				count++;
				return readReplica(replica);
			}else
				count =0;
		}
		
		return null;
	}
	
	/**
	 * Part d) Implement this method.
	 * send WriteRequestMessage to all replicas in the quorum
	 *  and receive an ACK 
	 *  if not all replicas replied try again for a specific no. of replies else stop 
	 *  
	 */
	protected void writeReplicas(Collection<SocketAddress> lockedReplicas, VersionedValue<T> newValue) {
		// TODO: Implement me!
		WriteRequestMessage<T> msg = new WriteRequestMessage<T>(newValue);
		for (Iterator iterator = lockedReplicas.iterator(); iterator.hasNext();) {
			SocketAddress socketAddress = (SocketAddress) iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, msg, socketAddress);
				PacketModel model = PacketSendReceive.receivePacket(socket,TIMEOUT);
				if(model.getData() instanceof Vote){
					iterator.remove();					
				}
				count =0;
			}catch(TimeoutException ex){
				System.out.println("Time out on write trying again");
				if(count < RETRIES){
					count++;
					writeReplicas(lockedReplicas, newValue);
				}else					
					count =0;
			}
		}
	}
	
	/**
	 * Part c) Implement this method (and checkQuorum(), see below) to read the
	 * replicated value using the majority consensus protocol.
	 * 1- request readvote
	 * 2- if valid quorum reached loop it to extract the replica address with the max version no.
	 * 3- read the value in the replica with the maxversion
	 * 4- store the versioned value and release readlocks  
	 *  here I am handling the situation where the Quorum is not reached by sending release readlock to all replicas that replied with yes
	 */
	public VersionedValue<T> get() throws QuorumNotReachedException {
		// TODO: Implement me!
		try{
			SocketAddress maxVersionAddress=null;
			int version=-1;
			Collection quiorum =requestReadVote();
			ArrayList<SocketAddress> adrses= new ArrayList<SocketAddress>();
			for (Iterator iterator = quiorum.iterator(); iterator.hasNext();) {
				MessageWithSource<Vote> msg = (MessageWithSource<Vote>) iterator.next();
				if(msg.getMessage().getVersion() >version){
					version = msg.getMessage().getVersion();
					maxVersionAddress=msg.getSource();
				}
				adrses.add(msg.getSource());
			}
			VersionedValue<T> value = new VersionedValue<T>(version,readReplica(maxVersionAddress));
			synchronized (adrses) {
				releaseReadLock(adrses);	
			}
			
			return value; 
		}catch(QuorumNotReachedException ex){
			releaseReadLock(ex.achieved);
			throw ex;
		}
			
	}

	/**
	 * Part d) Implement this method to set the
	 * replicated value using the majority consensus protocol.
	 * 1- request writevote
	 * 2- if valid quorum reached loop it to extract the replica address with the max version no.
	 * 3- create a new versioned value with the value to write and version number = maxversion + 1
	 * 4- send write request to all replicas in quorom 
	 * 5- release the writelock  
	 * here I am handling the situation where the Quorum is not reached by sending release writelock to all replicas that replied with yes
	 */
	public void set(T value) throws QuorumNotReachedException {
		// TODO: Implement me!
		try{
			SocketAddress maxVersionAddress=null;
			int version=-1;
			Collection quiorum =requestWriteVote();
			ArrayList<SocketAddress> srcs = new ArrayList<SocketAddress>();
			for (Iterator iterator = quiorum.iterator(); iterator.hasNext();) {
				MessageWithSource<Vote> msg = (MessageWithSource<Vote>) iterator.next();
				if(msg.getMessage().getVersion() >version){
					version = msg.getMessage().getVersion();
				}
				srcs.add(msg.getSource());
			}
			writeReplicas((Collection<SocketAddress>)srcs.clone(), new VersionedValue<T>(++version,value));
			System.out.println("Sending release msgs");
			releaseWriteLock(srcs);
		}catch(QuorumNotReachedException ex){
			releaseWriteLock(ex.achieved);
			throw ex;
		}
		
	}

	/**
	 * Part c) Implement this method to check whether a sufficient number of
	 * replies were received. If a sufficient number was received, this method
	 * should return the {@link MessageWithSource}s of the locked {@link Replica}s.
	 * Otherwise, a QuorumNotReachedException must be thrown.
	 * @throws QuorumNotReachedException 
	 */
	protected Collection<MessageWithSource<Vote>> checkQuorum(
			Collection<MessageWithSource<Vote>> replies) throws QuorumNotReachedException {
		// TODO: Implement me!
		// calculating the rquired no. of replicas
		int required = replicas.size()%2==0?(replicas.size()/2)+1:(replicas.size()+1)/2; 
		if(replies.size()>=required){
			//if valid return the current one 
			return replies;
		}else {
			//else extract the socket socketaddress to throw exception
			ArrayList<SocketAddress> achieved = new ArrayList<SocketAddress>();
			for (Iterator iterator = replies.iterator(); iterator.hasNext();) {
				MessageWithSource<Vote> messageWithSource = (MessageWithSource<Vote>) iterator.next();
				achieved.add(messageWithSource.getSource());
				
			}
			throw new QuorumNotReachedException(required, achieved);
		}
	}
	
	
}
