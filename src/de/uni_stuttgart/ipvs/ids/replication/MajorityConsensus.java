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

	public MajorityConsensus(Collection<SocketAddress> replicas)
			throws SocketException {
		this.replicas = replicas;
		SocketAddress address = new InetSocketAddress("127.0.0.1", 4999);
		this.socket = new DatagramSocket(address);
		this.nbio = new NonBlockingReceiver(socket);
	}

	/**
	 * Part c) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestReadVote() throws QuorumNotReachedException {
		// TODO: Implement me!
		ArrayList<MessageWithSource<Vote>> quiriom = new ArrayList<MessageWithSource<Vote>>();
		RequestReadVote request = new RequestReadVote();
		for (Iterator<SocketAddress> iterator = replicas.iterator(); iterator.hasNext();) {
			SocketAddress replicaAddress= iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, request, replicaAddress,TIMEOUT);				
				PacketModel received = PacketSendReceive.receivePacket(socket);			
				if(received.getData() instanceof Vote && received.getAddress().equals(replicaAddress)){
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
	 */
	protected void releaseReadLock(Collection<SocketAddress> lockedReplicas) {
		// TODO: Implement me!
		ReleaseReadLock msg = new ReleaseReadLock();
		for (Iterator iterator = lockedReplicas.iterator(); iterator.hasNext();) {
			SocketAddress socketAddress = (SocketAddress) iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, msg, socketAddress,TIMEOUT);
				PacketModel model = PacketSendReceive.receivePacket(socket);
				if(model.getData() instanceof Vote){
					iterator.remove();					
				}
			}catch(TimeoutException e){
				System.out.println("Time out on read release lock trying again");
				releaseReadLock(lockedReplicas);
			}
		}
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestWriteVote() throws QuorumNotReachedException {
		// TODO: Implement me!
		ArrayList<MessageWithSource<Vote>> quiriom = new ArrayList<MessageWithSource<Vote>>();
		RequestWriteVote request = new RequestWriteVote();
		for (Iterator<SocketAddress> iterator = replicas.iterator(); iterator.hasNext();) {
			SocketAddress replicaAddress= iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, request, replicaAddress,TIMEOUT);				
				PacketModel received = PacketSendReceive.receivePacket(socket);			
				if(received.getData() instanceof Vote && received.getAddress().equals(replicaAddress)){
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
	 */
	protected void releaseWriteLock(Collection<SocketAddress> lockedReplicas) {
		// TODO: Implement me!
		ReleaseWriteLock msg = new ReleaseWriteLock();
		for (Iterator iterator = lockedReplicas.iterator(); iterator.hasNext();) {
			SocketAddress socketAddress = (SocketAddress) iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, msg, socketAddress,TIMEOUT);
				System.out.println("sending release");
				PacketModel model = PacketSendReceive.receivePacket(socket);
				if(model.getData() instanceof Vote){
					iterator.remove();	
					System.out.println("received ack");
				}
				
			}catch(TimeoutException ex){
				System.out.println("Time out on release write lock trying again");
				releaseWriteLock(lockedReplicas);
			}
		}
	}
	
	/**
	 * Part c) Implement this method.
	 */
	protected T readReplica(SocketAddress replica) {
		// TODO: Implement me!
		try{
			ReadRequestMessage msg = new ReadRequestMessage();
			PacketSendReceive.sendPacket(socket, msg, replica,TIMEOUT);
			PacketModel received = PacketSendReceive.receivePacket(socket);	
			
			if(received.getData() instanceof ValueResponseMessage<?> && received.getAddress().equals(replica)){
				ValueResponseMessage<T> response = (ValueResponseMessage<T>) received.getData();
				return response.getValue();				
			}	
		}catch(TimeoutException ex){
			readReplica(replica);
		}
		
		return null;
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected void writeReplicas(Collection<SocketAddress> lockedReplicas, VersionedValue<T> newValue) {
		// TODO: Implement me!
		WriteRequestMessage<T> msg = new WriteRequestMessage<T>(newValue);
		for (Iterator iterator = lockedReplicas.iterator(); iterator.hasNext();) {
			SocketAddress socketAddress = (SocketAddress) iterator.next();
			try{
				PacketSendReceive.sendPacket(socket, msg, socketAddress,TIMEOUT);
				PacketModel model = PacketSendReceive.receivePacket(socket);
				if(model.getData() instanceof Vote){
					iterator.remove();					
				}
				
			}catch(TimeoutException ex){
				System.out.println("Time out on write trying again");
				releaseWriteLock(lockedReplicas);
			}
		}
	}
	
	/**
	 * Part c) Implement this method (and checkQuorum(), see below) to read the
	 * replicated value using the majority consensus protocol.
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
		int required = replicas.size()%2==0?(replicas.size()/2)+1:(replicas.size()+1)/2; 
		if(replies.size()>=required){
			return replies;
		}else {
			ArrayList<SocketAddress> achieved = new ArrayList<SocketAddress>();
			for (Iterator iterator = replies.iterator(); iterator.hasNext();) {
				MessageWithSource<Vote> messageWithSource = (MessageWithSource<Vote>) iterator.next();
				achieved.add(messageWithSource.getSource());
				
			}
			throw new QuorumNotReachedException(required, achieved);
		}
	}
	
	
}
