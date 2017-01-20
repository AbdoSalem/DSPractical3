package de.uni_stuttgart.ipvs.ids.communication;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import javax.naming.OperationNotSupportedException;

import de.uni_stuttgart.ipvs.ids.communication.Vote.State;

/**
 * Part b) Extend the method receiveMessages to return all DatagramPackets that
 * were received during the given timeout.
 * 
 * Also implement unpack() to conveniently convert a Collection of
 * DatagramPackets containing ValueResponseMessages to a collection of
 * MessageWithSource objects.
 * 
 */
public class NonBlockingReceiver {

	protected DatagramSocket socket;

	public NonBlockingReceiver(DatagramSocket socket) {
		this.socket = socket;
	}
	//the number of replicas
	public static int replicas ;
	
	//receive messages from all replicas with timeout and if expected number of messages break
	public Vector<DatagramPacket> receiveMessages(int timeoutMillis, int expectedMessages)
			throws IOException{
		// TODO: Impelement me!
		Vector<DatagramPacket> packets = new Vector<DatagramPacket>();
		for(int i = 0;i< replicas;i++){
			try{
				packets.add(PacketSendReceive.receiveDGPacket(socket,timeoutMillis));
				if(packets.size()>=expectedMessages)
					break;
			}catch(TimeoutException ex){
				
			}
		}
		return packets;
		
	}
	
	public static <T> Collection<MessageWithSource<T>> unpack(
			Collection<DatagramPacket> packetCollection) throws IOException,
			ClassNotFoundException {
		// TODO: Impelement me!
		ArrayList<MessageWithSource<T>> msgs = new ArrayList<MessageWithSource<T>>();
		for (Iterator iterator = packetCollection.iterator(); iterator.hasNext();) {
			DatagramPacket datagramPacket = (DatagramPacket) iterator.next();
			ByteArrayInputStream b = new ByteArrayInputStream(datagramPacket.getData());
			ObjectInputStream o = new ObjectInputStream(b);
			msgs.add(new MessageWithSource<T>(datagramPacket.getSocketAddress(),(T)o.readObject()));
		}
		return msgs;
	}
	
}
