package de.uni_stuttgart.ipvs.ids.communication;

import java.net.SocketAddress;

public class PacketModel{
	private SocketAddress address;
	private Object data;
	
	public PacketModel(SocketAddress address, Object data) {
		super();
		this.address = address;
		this.data = data;
	}
	public SocketAddress getAddress() {
		return address;
	}
	public void setAddress(SocketAddress address) {
		this.address = address;
	}
	public Object getData() {
		return data;
	}
	public void setData(Object data) {
		this.data = data;
	}
	
}