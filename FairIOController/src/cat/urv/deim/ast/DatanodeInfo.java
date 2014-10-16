package cat.urv.deim.ast;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

//import org.apfloat.Apfloat;

public class DatanodeInfo implements Comparable<DatanodeInfo>{

	private DatanodeID nodeID;
	private BigDecimal capacity; // I/O max throughput in Mb/s
	private Map<ClassInfo, BigDecimal> weightByClass;
	private BigDecimal totalWeight;
	
	public DatanodeInfo(DatanodeID nodeID, float capacity) {
		this.nodeID = nodeID;
		this.capacity = new BigDecimal(capacity);
		this.weightByClass = new HashMap<ClassInfo, BigDecimal>();
		this.totalWeight = BigDecimal.ZERO;
	}
	
	public DatanodeID getDatanodeID() {
		return this.nodeID;
	}
	
	public BigDecimal getCapacity() {
		return this.capacity;
	}
	
	public BigDecimal getClassWeight(ClassInfo classInfo) {
		return this.weightByClass.get(classInfo);
	}
	
	public void updateClassWeight(ClassInfo classInfo, BigDecimal newWeight) {
		BigDecimal oldWeight = this.weightByClass.put(classInfo, newWeight);
		if (oldWeight == null) oldWeight = BigDecimal.ZERO;
		this.totalWeight = this.totalWeight.subtract(oldWeight).add(newWeight);
	}
	
	public BigDecimal getClassShare(ClassInfo classInfo) {
		if (this.totalWeight.equals(BigDecimal.ZERO))
			return BigDecimal.ZERO;
		return this.getClassWeight(classInfo).divide(this.totalWeight, FairIOController.CONTEXT);
	}
	
	public BigDecimal getTotalWeight() {
		if (this.totalWeight.equals(BigDecimal.ZERO)) {
			return FairIOController.MIN_TOTAL_WEIGHT;
		}
		return this.totalWeight;
	}
	
	public BigDecimal getMarginalValue() {
		if (this.totalWeight.equals(BigDecimal.ZERO))
			return BigDecimal.ZERO;
		return this.capacity.divide(this.totalWeight, FairIOController.CONTEXT);
	}
	
	public String toString() {
		return String.format("[nid: %s, cap: %s]", nodeID, FairIOController.decimalFormat.format(capacity));
	}
	
	
	@Override
	public int compareTo(DatanodeInfo o) {
		return this.nodeID.compareTo(o.getDatanodeID());
	}
	
	
	
}
