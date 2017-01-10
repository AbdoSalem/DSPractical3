package de.uni_stuttgart.ipvs.ids.communication;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Vector;

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

	public Vector<DatagramPacket> receiveMessages(int timeoutMillis, int expectedMessages)
			throws IOException {
		// TODO: Impelement me!	
	}

	public static <T> Collection<MessageWithSource<T>> unpack(
			Collection<DatagramPacket> packetCollection) throws IOException,
			ClassNotFoundException {
		// TODO: Impelement me!	
	}
	
}
