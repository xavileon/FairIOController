package cat.urv.deim.ast;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.DatanodeID;

public class DatanodeInfo implements Comparable<DatanodeInfo>{

	private DatanodeID nodeID;
	private BigDecimal capacity; // I/O max throughput in Mb/s
	private Map<ClassInfo, BigDecimal> weightByClass;
	private BigDecimal totalWeight;
	
	public DatanodeInfo(DatanodeID nodeID) {
		this(nodeID, 100);
	}
	
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
		if (!this.weightByClass.containsKey(classInfo))
			return BigDecimal.ZERO;
		return this.weightByClass.get(classInfo);
	}
	
	public void updateClassWeight(ClassInfo classInfo, BigDecimal newWeight) {
		BigDecimal oldWeight = this.weightByClass.put(classInfo, newWeight);
		if (oldWeight == null) oldWeight = BigDecimal.ZERO;
		if (newWeight.compareTo(BigDecimal.ZERO) == 0) this.weightByClass.remove(classInfo);
		this.totalWeight = this.totalWeight.subtract(oldWeight).add(newWeight);
	}
	
	public BigDecimal getClassShare(ClassInfo classInfo) {
		if (!this.weightByClass.containsKey(classInfo))
			return BigDecimal.ZERO;
		else if (this.weightByClass.size() == 1)
			return BigDecimal.ONE;
		return this.getClassWeight(classInfo).divide(this.getTotalWeight(), FairIOController.CONTEXT);
	}
	
	public BigDecimal getTotalWeight() {
		if (this.weightByClass.size() <= 1) {
			return FairIOController.MIN_TOTAL_WEIGHT.multiply(this.capacity);
		}
		return this.totalWeight;
	}
	
	public BigDecimal getMarginalValue() {
		return this.capacity.divide(this.getTotalWeight(), FairIOController.CONTEXT);
	}
	
	public String toString() {
		return String.format("[nid: %s, cap: %s, marginal: %s]", nodeID, FairIOController.decimalFormat.format(capacity), FairIOController.decimalFormat.format(this.getMarginalValue()));
	}
	
	@Override
	public int compareTo(DatanodeInfo o) {
		return this.nodeID.compareTo(o.getDatanodeID());
	}
	
	@Override
	public boolean equals(Object o) {
		return this.nodeID.equals(((DatanodeInfo)o).nodeID);
	}
	
	@Override
	public int hashCode() {
		return this.nodeID.hashCode();
	}
	
	
	
}
