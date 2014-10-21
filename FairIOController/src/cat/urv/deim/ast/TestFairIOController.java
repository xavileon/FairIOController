package cat.urv.deim.ast;

import java.math.BigDecimal;


public class TestFairIOController {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FairIOController controller = new FairIOController();
		
		controller.computeShares();
		
		DatanodeInfo d1 = new DatanodeInfo(new DatanodeID(), 200);
		DatanodeInfo d2 = new DatanodeInfo(new DatanodeID(), 200);
		DatanodeInfo d3 = new DatanodeInfo(new DatanodeID(), 200);
		ClassInfo c1 = new ClassInfo(1);
		c1.setWeight(new BigDecimal(100));
		ClassInfo c2 = new ClassInfo(2);
		c2.setWeight(new BigDecimal(200));
		ClassInfo c3 = new ClassInfo(3);
		c3.setWeight(new BigDecimal(400));
		ClassInfo c4 = new ClassInfo(4);
		c4.setWeight(new BigDecimal(400));
		// Give an initial weight of 100 (arbitrary)
		// We should check the weight of the file on the namenode
		controller.addDatanodeToClass(c1, d1);
		
		controller.computeShares();
		System.out.println("AFTER adding c1\n"+ controller);
		
		controller.addDatanodeToClass(c2, d1);
		controller.addDatanodeToClass(c2, d2);
		controller.addDatanodeToClass(c2, d3);
		
		controller.computeShares();
		System.out.println("AFTER adding c2\n"+ controller);
		controller.addDatanodeToClass(c3, d1);
		controller.computeShares();
		System.out.println("AFTER adding c3\n"+ controller);
		
		controller.addDatanodeToClass(c4, d2);
		controller.computeShares();
		System.out.println("AFTER adding c4 to d2\n"+ controller);
		controller.addDatanodeToClass(c4, d3);
		controller.computeShares();
		System.out.println("AFTER adding c4 to d3\n"+ controller);

	}

}
