package de.uni_stuttgart.ipvs.ids.replication;

import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class Main {

	/**
	 * @param args
	 * @throws SocketException 
	 * @throws QuorumNotReachedException 
	 */
	public static void main(String[] args) throws SocketException, QuorumNotReachedException {
		int replicaPort = 4000;
		int noReplicas = 10;
		double prob = 0.5; // Probability for working replica
		double value = 2.0;

		List<Replica<Double>> replicas = new ArrayList<Replica<Double>>(noReplicas);
		List<SocketAddress> replicaAddrs = new ArrayList<SocketAddress>(replicas.size());
		
		for (int i = 0; i < noReplicas; i++) {
			Replica<Double> r = new Replica<Double>(i, replicaPort + i, prob, value);
			r.start();
			replicas.add(r);
			replicaAddrs.add(r.getSocketAddress());
		}

		MajorityConsensus<Double> mc = new MajorityConsensus<Double>(replicaAddrs);
		
		try {
			double y = mc.get().getValue();

			System.out.println("MajorityConsensus: y is " + y);
			mc.set(3.0);

			y = mc.get().getValue();

			System.out.println("MajorityConsensus: y is " + y);
		} catch (Exception e) {
			System.err.println("Exception occured: " + e.getMessage());
			e.printStackTrace();
		} finally {
			System.exit(0);
		}
	}
}
