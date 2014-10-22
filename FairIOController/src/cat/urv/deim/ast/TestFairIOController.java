package cat.urv.deim.ast;

import org.apache.hadoop.hdfs.protocol.DatanodeID;


public class TestFairIOController {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		FairIOController controller = new FairIOController();
		
		controller.computeShares();
		System.out.println("AFTER with nothing \n"+ controller);
		//String ipAddr, String hostName, String datanodeUuid, int xferPort, int infoPort, int infoSecurePort, int ipcPort
		DatanodeID id1 = new DatanodeID("192.168.0.1", "d1", "d1", 1234, 1235, 1236, 1237);
		DatanodeID id2 = new DatanodeID("192.168.0.2", "d2", "d2", 1234, 1235, 1236, 1237);
		DatanodeID id3 = new DatanodeID("192.168.0.3", "d3", "d3", 1234, 1235, 1236, 1237);
		
		controller.registerDatanode(id1);
		controller.registerDatanode(id2);
		controller.registerDatanode(id3);
		
		if (!controller.existsClassInfo(1)) {
			// Rely on default value
			controller.setClassWeight(1);
		}
		controller.setClassWeight(2,  200);
		controller.setClassWeight(3,  400);
		controller.setClassWeight(4,  400);
		
		controller.computeShares();
		System.out.println("AFTER registering without adding nodes \n"+ controller);
		
		// Give an initial weight of 100 (arbitrary)
		// We should check the weight of the file on the namenode
		controller.addDatanodeToClass(1, "d1");
		
		controller.computeShares();
		System.out.println("AFTER adding c1\n"+ controller);
		
		controller.addDatanodeToClass(2, "d1");
		controller.addDatanodeToClass(2, "d2");
		controller.addDatanodeToClass(2, "d3");
		
		controller.computeShares();
		System.out.println("AFTER adding c2\n"+ controller);
		controller.addDatanodeToClass(3, "d1");
		controller.computeShares();
		System.out.println("AFTER adding c3\n"+ controller);
		
		controller.addDatanodeToClass(4, "d2");
		controller.computeShares();
		System.out.println("AFTER adding c4 to d2\n"+ controller);
		controller.addDatanodeToClass(4, "d3");
		controller.computeShares();
		System.out.println("AFTER adding c4 to d3\n"+ controller);

	}

}
