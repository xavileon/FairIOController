package cat.urv.deim.ast;

import java.util.HashMap;
import java.util.Map;

import org.apfloat.Apfloat;

public class DatanodeInfo implements Comparable<DatanodeInfo>{

	private DatanodeID nodeID;
	private Apfloat capacity; // I/O max throughput in Mb/s
	private Map<ClassInfo, Apfloat> weightByClass;
	private Apfloat totalWeight;
	
	public DatanodeInfo(DatanodeID nodeID, float capacity) {
		this.nodeID = nodeID;
		this.capacity = new Apfloat(capacity, 12);
		this.weightByClass = new HashMap<ClassInfo, Apfloat>();
		this.totalWeight = Apfloat.ZERO;
	}
	
	public DatanodeID getDatanodeID() {
		return this.nodeID;
	}
	
	public Apfloat getCapacity() {
		return this.capacity;
	}
	
	public Apfloat getClassWeight(ClassInfo classInfo) {
		return this.weightByClass.get(classInfo);
	}
	
	public void updateClassWeight(ClassInfo classInfo, Apfloat newWeight) {
		Apfloat oldWeight = this.weightByClass.put(classInfo, newWeight);
		if (oldWeight == null) oldWeight = Apfloat.ZERO;
		this.totalWeight = this.totalWeight.subtract(oldWeight).add(newWeight);
	}
	
	public Apfloat getClassShare(ClassInfo classInfo) {
		if (this.totalWeight.equals(Apfloat.ZERO))
			return Apfloat.ZERO;
		return this.getClassWeight(classInfo).divide(this.totalWeight);
	}
	
	public Apfloat getTotalWeight() {
		return this.totalWeight;
	}
	
	public Apfloat getMarginalValue() {
		if (this.totalWeight.equals(Apfloat.ZERO))
			return Apfloat.ZERO;
		return this.capacity.divide(this.totalWeight);
	}
	
	public String toString() {
		return String.format("[nid: %s, cap: %#s]", nodeID, capacity);
	}
	
	
	@Override
	public int compareTo(DatanodeInfo o) {
		return this.nodeID.compareTo(o.getDatanodeID());
	}
	
	
	
}
