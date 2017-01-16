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
	public static void sendPacket(DatagramSocket socket,Object toSend, SocketAddress address) {
		try{
		sendPacket(socket, toSend, address,0);
		}catch(Exception ex){
			System.out.println("TimeOUT excception");
			ex.printStackTrace();
		}
	}
	public static void sendPacket(DatagramSocket socket,Object toSend, SocketAddress address, int TIMEOUT) throws TimeoutException{
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
			socket.setSoTimeout(TIMEOUT);
			}catch(IOException e){
				e.printStackTrace();
			}
		
	}
	public static PacketModel receivePacket(DatagramSocket socket){
		
		DatagramPacket dummyPacket = new DatagramPacket(new byte[1024], 1024);
		
		try {
			socket.receive(dummyPacket);
			// create inout streams to deserialize byte[] to object
			ByteArrayInputStream b = new ByteArrayInputStream(dummyPacket.getData());
			ObjectInputStream o = new ObjectInputStream(b);			
			return new PacketModel(new InetSocketAddress(dummyPacket.getAddress(), dummyPacket.getPort()), o.readObject());

		} catch (Exception e) {
			System.out.println("Exception occured while reading packet");
			e.printStackTrace();
			return null; // Pacify the compiler
		}
	}
}

