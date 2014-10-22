package cat.urv.deim.ast;

import java.math.BigDecimal;

public class ClassInfo implements Comparable<ClassInfo>{
	
	private long classID;
	private BigDecimal weight;
	
	public ClassInfo(long classID) {
		this.classID = classID;
		this.weight = new BigDecimal(100);
	}
	
	public ClassInfo(long classID, float weight) {
		this.classID = classID;
		this.weight = new BigDecimal(weight);
	}
	
	public long getClassID() {
		return this.classID;
	}
	
	public BigDecimal getWeight() {
		return this.weight;
	}
	
	public void setWeight(BigDecimal weight) {
		this.weight = weight;
	}

	public String toString() {
		return String.format("[class: %s, weight: %s]", classID, FairIOController.decimalFormat.format(weight));
	}
	
	@Override
	public int compareTo(ClassInfo o) {
		if (this.classID < o.getClassID()) return -1;
		else if (this.classID == o.getClassID()) return 0;
		else return 1;
	}
	
	public boolean equals(Object o) {
		if (this.classID == ((ClassInfo)o).getClassID()) return true;
		else return false;
	}
	
	public int hashCode() {
		return new Long(this.classID).hashCode();
	}
	
}
