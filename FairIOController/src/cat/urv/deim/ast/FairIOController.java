package cat.urv.deim.ast;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apfloat.Apfloat;
import org.apfloat.ApfloatMath;


public class FairIOController {

	// Private constants taken from configuration file
	private static final int APFLOAT_PRECISSION = 12;
	private static final Apfloat MIN_UTILITY_GAP = new Apfloat(0.001, APFLOAT_PRECISSION);
	
	// Private constants for ease code reading
	private static final Apfloat MIN_SHARE = new Apfloat(0.0001, APFLOAT_PRECISSION);
	private static final Apfloat ONE_MINUS_MIN_SHARE = Apfloat.ONE.subtract(MIN_SHARE);
	private static final Apfloat MIN_COEFF = MIN_SHARE.divide(ONE_MINUS_MIN_SHARE);
	
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
				datanode.updateClassWeight(classInfo, Apfloat.ZERO);
				// Remove the class from memory if no more datanodes
				if (this.classToDatanodes.size() == 0)
					this.classToDatanodes.remove(classInfo);
			}
		}
	}
	
	/* Compute the corresponding shares for all classids */
	public void computeShares() {
		HashMap<ClassInfo, Apfloat> previousUtilities = new HashMap<ClassInfo, Apfloat>();
		
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
			Apfloat initWeight = classInfo.getWeight().divide(new Apfloat(numDatanodes, APFLOAT_PRECISSION));
			datanode.updateClassWeight(classInfo, initWeight);
		}
	}
	
	public String toString() {
		String res = "ClassInfo-DatanodeInfo Status\n";
		res += "-----------------------------\n";
		for (ClassInfo classInfo : classToDatanodes.keySet()) {
			res += "ClassInfo: "+classInfo+ "\n";
			for (DatanodeInfo datanode : classToDatanodes.get(classInfo)) {
				res += String.format("\t Datanode %s: %#s %#s\n", datanode, datanode.getClassWeight(classInfo), datanode.getClassShare(classInfo));
			}
		}
		return res;
	}
	
	private Apfloat getUtility(ClassInfo classInfo) {
		Apfloat utility = Apfloat.ZERO;
		for (DatanodeInfo datanode : this.classToDatanodes.get(classInfo)) {
			Apfloat cj = datanode.getCapacity();
			Apfloat sij = datanode.getClassShare(classInfo);
			// sum_ut += disk.capacity * disk.get_share_byfile(fid)
			utility = utility.add(cj.multiply(sij));
		}
		return utility;
	}
	
	private Map<ClassInfo, Apfloat> getUtilities() {
		HashMap<ClassInfo, Apfloat> utilities = new HashMap<ClassInfo, Apfloat>();
		for (ClassInfo classInfo : classToDatanodes.keySet()) {
			utilities.put(classInfo, getUtility(classInfo));
		}
		return utilities;
	}
	
	/* Return wether the current utility of all classes differ less than
	 * min utility gap wrt previous utility.
	 */
	private boolean isUtilityConverged(Map<ClassInfo, Apfloat> previousUtilities) {
		boolean converged = true;
		Map<ClassInfo, Apfloat> currentUtilities = getUtilities();
		// no previous utilities, so update with current ones and return not converged
		if (previousUtilities.isEmpty()) {
			previousUtilities.putAll(currentUtilities);
			return false;
		}
		
		// Use current utilities to compare with previousUtilities
		for (ClassInfo classInfo : currentUtilities.keySet()) {
			Apfloat currentUtility = currentUtilities.get(classInfo);
			Apfloat previousUtility = previousUtilities.get(classInfo);
			Apfloat utilityGap = ApfloatMath.abs(
					currentUtility.subtract(previousUtility));
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
		Apfloat sub_total_mins = Apfloat.ZERO;
		for (DatanodeInfo datanode : datanodes) {
			Apfloat yj = datanode.getTotalWeight();
			sub_total_mins = sub_total_mins.add(yj.multiply(MIN_COEFF));
		}
		Apfloat budget = classInfo.getWeight();
		Apfloat sub_total_sqrt_wy = Apfloat.ZERO;
		Apfloat sub_total_y = Apfloat.ZERO;
		Apfloat last_coeff = Apfloat.ZERO;
		int k = 0;
		
		for (DatanodeInfo datanode : datanodes) {
			Apfloat yj = datanode.getTotalWeight(); // total weights on this datanode, price
			Apfloat cj = datanode.getCapacity(); // capacity for this datanode
			// sqrt_wy = (yj * cj).sqrt()
			Apfloat sqrt_wy = ApfloatMath.sqrt(yj.multiply(cj));
			// sub_total_sqrt_wy += sqrt_wy;
			sub_total_sqrt_wy = sub_total_sqrt_wy.add(sqrt_wy);
			// sub_total_y += yj;
			sub_total_y = sub_total_y.add(yj);
			// sub_total_mins -= yj*min_coeff
			sub_total_mins = sub_total_mins.subtract(yj.multiply(MIN_COEFF));
			// coeff = (budget - sub_total_mins + sub_total_y) / sub_total_sqrt_wy;
			Apfloat coeff = budget.subtract(sub_total_mins)
									.add(sub_total_y)
									.divide(sub_total_sqrt_wy);
			// t = (sqrt_wy * coeff) - yj
			Apfloat t = sqrt_wy.multiply(coeff).subtract(yj);
			//tmin = t - ((min_share*yj / (1 - min_share))
			Apfloat tmin = t.subtract(MIN_SHARE.multiply(yj)
									           .divide(ONE_MINUS_MIN_SHARE));
			// if tmin >= 0
			if (tmin.compareTo(Apfloat.ZERO) >= 0) {
				k++;
				last_coeff = coeff;
			}
			else
				break;
		}
		
		// Update weight on each node with xij higher than min_coeff
		for (int i = 0; i < k; i++) {
			DatanodeInfo datanode = datanodes.get(i);
			Apfloat yj = datanode.getTotalWeight();
			Apfloat cj = datanode.getCapacity();
			// xij = ((yj*cj).sqrt() * last_coeff) - yj
			Apfloat xij = ApfloatMath.sqrt(yj.multiply(cj))
									 .multiply(last_coeff)
									 .subtract(yj);
			datanode.updateClassWeight(classInfo, xij);
		}
		// Update the rest of nodes with an xij = min_coeff
		for (int i = k; i < datanodes.size(); i++) {
			DatanodeInfo datanode = datanodes.get(i);
			Apfloat yj = datanode.getTotalWeight();
			// xij = yj * min_coeff
			Apfloat xij = yj.multiply(MIN_COEFF);
			datanode.updateClassWeight(classInfo, xij);
		}
	}
}