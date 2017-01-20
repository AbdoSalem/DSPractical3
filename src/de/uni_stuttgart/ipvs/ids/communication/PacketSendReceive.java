package de.uni_stuttgart.ipvs.ids.communication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeoutException;

public class PacketSendReceive {
	
	/*
	 * This method creates a datagram packet with the to send object and send it
	 * to the input address
	 */
	public static void sendPacket(DatagramSocket socket,Object toSend, SocketAddress address){
		byte[] data;
			try{
			// convert object to byte[]
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			ObjectOutputStream o = new ObjectOutputStream(b);
			o.writeObject(toSend);
			data = b.toByteArray();
			
			// create the data packet and pass the byte[] to it and set
			// address to passed address
			DatagramPacket packet = new DatagramPacket(data, data.length);
			packet.setSocketAddress(address);
//			System.out.println("Sending "+toSend.toString() +" to "+packet.getAddress().getHostName()+":"+packet.getPort());
			// send packet
			socket.send(packet);
			
			}catch(IOException e){
				e.printStackTrace();
			}
		
	}
    //Receive a packet without timeout and extract the packet model
	public static PacketModel receivePacket(DatagramSocket socket) {
		try{
			return 	receivePacket(socket,0);
		}catch(Exception e){
			return null;
		}
		
	}
	//Receive a packet and extract the packet model from it with a timeout
	public static PacketModel receivePacket(DatagramSocket socket, int TIMEOUT) throws TimeoutException{
		
		DatagramPacket dummyPacket = new DatagramPacket(new byte[1024], 1024);
		
		try {
			socket.setSoTimeout(TIMEOUT);
			socket.receive(dummyPacket);
			// create inout streams to deserialize byte[] to object
			ByteArrayInputStream b = new ByteArrayInputStream(dummyPacket.getData());
			ObjectInputStream o = new ObjectInputStream(b);			
			return new PacketModel(new InetSocketAddress(dummyPacket.getAddress(), dummyPacket.getPort()), o.readObject());

		} catch (IOException e) {
			System.out.println("Exception occured while reading packet");
			e.printStackTrace();
			return null; // Pacify the compiler
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	//receive a packet with timeout and return the packet as it is
	public static DatagramPacket receiveDGPacket(DatagramSocket socket, int TIMEOUT) throws TimeoutException{
		
		DatagramPacket dummyPacket = new DatagramPacket(new byte[1024], 1024);
		
		try {
			socket.setSoTimeout(TIMEOUT);
			socket.receive(dummyPacket);
			// create inout streams to deserialize byte[] to object
					
			return dummyPacket;

		} catch (IOException e) {
			System.out.println("Exception occured while reading packet");
			e.printStackTrace();
			return null; // Pacify the compiler
		}
	}
}

