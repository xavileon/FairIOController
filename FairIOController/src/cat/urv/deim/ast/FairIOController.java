package cat.urv.deim.ast;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FairIOController {
	
	public static final int PRECISSION = 32;
	public static MathContext CONTEXT = new MathContext(PRECISSION);
	public static final BigDecimal MIN_TOTAL_WEIGHT = new BigDecimal(0.000000000000000001, CONTEXT);
	public static DecimalFormat decimalFormat = new DecimalFormat("0.00000");
	
	// Private constants taken from configuration file
	private static final BigDecimal MIN_UTILITY_GAP = new BigDecimal(0.001, CONTEXT);
	
	// Private constants for ease code reading
	private static final BigDecimal MIN_SHARE = new BigDecimal(0.0001, CONTEXT);
	private static final BigDecimal ONE_MINUS_MIN_SHARE = BigDecimal.ONE.subtract(MIN_SHARE, CONTEXT);
	private static final BigDecimal MIN_COEFF = MIN_SHARE.divide(ONE_MINUS_MIN_SHARE, CONTEXT);
	private static final BigDecimal TWO = new BigDecimal(2, CONTEXT);
	
	private Map<ClassInfo, Set<DatanodeInfo>> classToDatanodes;
	private Map<DatanodeID, DatanodeInfo> nodeIDtoInfo;
	private MarginalsComparator datanodeInfoComparator;
	
	public FairIOController() {
		this.classToDatanodes = new HashMap<ClassInfo, Set<DatanodeInfo>>();
		this.nodeIDtoInfo = new HashMap<DatanodeID, DatanodeInfo>();
		this.datanodeInfoComparator = new MarginalsComparator();
	}
	
	/* Create a datanode datatype identified with datanodeID to a given class */
	public void addDatanodeToClass(ClassInfo classInfo, DatanodeInfo datanode) {
		if (!this.nodeIDtoInfo.containsKey(datanode.getDatanodeID())) {
			this.nodeIDtoInfo.put(datanode.getDatanodeID(), datanode);
		}
		
		if (!this.classToDatanodes.containsKey(classInfo)) {
			this.classToDatanodes.put(classInfo, new HashSet<DatanodeInfo>());
			
		}
		
		this.classToDatanodes.get(classInfo).add(datanode);
	}
	
	/* remove the datanode from a given class, put its weight to ZERO because
	 * not interested anymore */
	public void removeDatanodeFromClass(ClassInfo classInfo, DatanodeID datanodeID) {
		if (this.classToDatanodes.containsKey(classInfo)) {
			DatanodeInfo datanode = this.nodeIDtoInfo.get(datanodeID);
			if (datanode != null) {
				this.classToDatanodes.get(classInfo).remove(datanode);
				datanode.updateClassWeight(classInfo, BigDecimal.ZERO);
				// Remove the class from memory if no more datanodes
				if (this.classToDatanodes.size() == 0)
					this.classToDatanodes.remove(classInfo);
			}
		}
	}
	
	/* Compute the corresponding shares for all classids */
	public void computeShares() {
		HashMap<ClassInfo, BigDecimal> previousUtilities = new HashMap<ClassInfo, BigDecimal>();
		
		while (!isUtilityConverged(previousUtilities)) {
			for (ClassInfo classInfo : classToDatanodes.keySet()) {
				computeSharesByClass(classInfo);
			}
			//System.out.println(this);
		}
	}
	
	public void initializeAllShares() {
		for (ClassInfo classInfo: classToDatanodes.keySet()) {
			initializeShares(classInfo);
		}
	}
	
	public void initializeShares(ClassInfo classInfo) {
		int numDatanodes = classToDatanodes.get(classInfo).size();
		for (DatanodeInfo datanode : classToDatanodes.get(classInfo)) {
			BigDecimal initWeight = classInfo.getWeight().divide(new BigDecimal(numDatanodes));
			datanode.updateClassWeight(classInfo, initWeight);
		}
	}
	
	public String toString() {
		String res = "ClassInfo-DatanodeInfo Status\n";
		res += "-----------------------------\n";
		for (ClassInfo classInfo : classToDatanodes.keySet()) {
			res += "ClassInfo: "+classInfo+ "\n";
			for (DatanodeInfo datanode : classToDatanodes.get(classInfo)) {
				BigDecimal classWeight = datanode.getClassWeight(classInfo);
				BigDecimal classShare = datanode.getClassShare(classInfo);
				res += String.format("\t Datanode %s: %s %s\n", datanode, FairIOController.decimalFormat.format(classWeight), FairIOController.decimalFormat.format(classShare));
			}
		}
		return res;
	}
	
	private BigDecimal getUtility(ClassInfo classInfo) {
		BigDecimal utility = BigDecimal.ZERO;
		for (DatanodeInfo datanode : this.classToDatanodes.get(classInfo)) {
			BigDecimal cj = datanode.getCapacity();
			BigDecimal sij = datanode.getClassShare(classInfo);
			// sum_ut += disk.capacity * disk.get_share_byfile(fid)
			utility = utility.add(cj.multiply(sij));
		}
		return utility;
	}
	
	private Map<ClassInfo, BigDecimal> getUtilities() {
		HashMap<ClassInfo, BigDecimal> utilities = new HashMap<ClassInfo, BigDecimal>();
		for (ClassInfo classInfo : classToDatanodes.keySet()) {
			utilities.put(classInfo, getUtility(classInfo));
		}
		return utilities;
	}
	
	/* Return wether the current utility of all classes differ less than
	 * min utility gap wrt previous utility.
	 */
	private boolean isUtilityConverged(Map<ClassInfo, BigDecimal> previousUtilities) {
		boolean converged = true;
		Map<ClassInfo, BigDecimal> currentUtilities = getUtilities();
		// no previous utilities, so update with current ones and return not converged
		if (previousUtilities.isEmpty()) {
			previousUtilities.putAll(currentUtilities);
			return false;
		}
		
		// Use current utilities to compare with previousUtilities
		for (ClassInfo classInfo : currentUtilities.keySet()) {
			BigDecimal currentUtility = currentUtilities.get(classInfo);
			BigDecimal previousUtility = previousUtilities.get(classInfo);
			BigDecimal utilityGap = currentUtility.subtract(previousUtility).abs();
			//System.out.printf("%s %#s", classInfo, utilityGap);
			if (utilityGap.compareTo(MIN_UTILITY_GAP) <= 0) {
				//System.out.printf(" CONVERGED\n");
				converged = converged && true;
			}
			else {
				//System.out.println(" NOT CONVERGED\n");
				converged = false;
			}
		}
		previousUtilities.clear();
		previousUtilities.putAll(currentUtilities);
		//System.out.println("FINAL ROUND: "+converged);
		return converged;
	}
	
	private void computeSharesByClass(ClassInfo classInfo) {
		ArrayList<DatanodeInfo> datanodes = new ArrayList<DatanodeInfo>(this.classToDatanodes.get(classInfo));
		Collections.sort(datanodes, this.datanodeInfoComparator);
		
		// Optimization algorithm per se
		//sub_total_mins = sum([yj*min_coeff for _, _, _, yj, _, _ in marginals])
		BigDecimal sub_total_mins = BigDecimal.ZERO;
		for (DatanodeInfo datanode : datanodes) {
			BigDecimal yj = datanode.getTotalWeight();
			sub_total_mins = sub_total_mins.add(yj.multiply(MIN_COEFF));
		}
		BigDecimal budget = classInfo.getWeight();
		BigDecimal sub_total_sqrt_wy = BigDecimal.ZERO;
		BigDecimal sub_total_y = BigDecimal.ZERO;
		BigDecimal last_coeff = BigDecimal.ZERO;
		int k = 0;
		
		for (DatanodeInfo datanode : datanodes) {
			BigDecimal yj = datanode.getTotalWeight(); // total weights on this datanode, price
			BigDecimal cj = datanode.getCapacity(); // capacity for this datanode
			// sqrt_wy = (yj * cj).sqrt()
			BigDecimal sqrt_wy = sqrt(yj.multiply(cj));
			// sub_total_sqrt_wy += sqrt_wy;
			sub_total_sqrt_wy = sub_total_sqrt_wy.add(sqrt_wy);
			// sub_total_y += yj;
			sub_total_y = sub_total_y.add(yj);
			// sub_total_mins -= yj*min_coeff
			sub_total_mins = sub_total_mins.subtract(yj.multiply(MIN_COEFF));
			// coeff = (budget - sub_total_mins + sub_total_y) / sub_total_sqrt_wy;
			BigDecimal coeff = budget.subtract(sub_total_mins)
									.add(sub_total_y)
									.divide(sub_total_sqrt_wy, CONTEXT);
			// t = (sqrt_wy * coeff) - yj
			BigDecimal t = sqrt_wy.multiply(coeff).subtract(yj);
			//tmin = t - ((min_share*yj / (1 - min_share))
			BigDecimal tmin = t.subtract(MIN_SHARE.multiply(yj)
									           .divide(ONE_MINUS_MIN_SHARE, CONTEXT));
			// if tmin >= 0
			if (tmin.compareTo(BigDecimal.ZERO) >= 0) {
				k++;
				last_coeff = coeff;
			}
			else
				break;
		}
		
		// Update weight on each node with xij higher than min_coeff
		for (int i = 0; i < k; i++) {
			DatanodeInfo datanode = datanodes.get(i);
			BigDecimal yj = datanode.getTotalWeight();
			BigDecimal cj = datanode.getCapacity();
			// xij = ((yj*cj).sqrt() * last_coeff) - yj
			BigDecimal xij = sqrt(yj.multiply(cj))
									 .multiply(last_coeff)
									 .subtract(yj);
			datanode.updateClassWeight(classInfo, xij);
		}
		// Update the rest of nodes with an xij = min_coeff
		for (int i = k; i < datanodes.size(); i++) {
			DatanodeInfo datanode = datanodes.get(i);
			BigDecimal yj = datanode.getTotalWeight();
			// xij = yj * min_coeff
			BigDecimal xij = yj.multiply(MIN_COEFF);
			datanode.updateClassWeight(classInfo, xij);
		}
	}
	
	private static BigDecimal sqrt(BigDecimal A) {
	    BigDecimal x0 = new BigDecimal(0, CONTEXT);
	    BigDecimal x1 = new BigDecimal(Math.sqrt(A.doubleValue()), CONTEXT);
	    while (!x0.equals(x1)) {
	        x0 = x1;
	        x1 = A.divide(x0, CONTEXT);
	        x1 = x1.add(x0);
	        x1 = x1.divide(TWO, CONTEXT);

	    }
	    return x1;
	}
}
