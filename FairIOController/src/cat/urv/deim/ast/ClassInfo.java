package cat.urv.deim.ast;

import org.apfloat.Apfloat;

public class ClassInfo implements Comparable<ClassInfo>{

	private static int APFLOAT_PRECISSION = 16; // get from configuration
	
	private long classID;
	private Apfloat weight;
	
	public ClassInfo(long classID) {
		this.classID = classID;
		this.weight = new Apfloat(100, APFLOAT_PRECISSION);
	}
	
	public long getClassID() {
		return this.classID;
	}
	
	public Apfloat getWeight() {
		return this.weight;
	}
	
	public void setWeight(Apfloat weight) {
		this.weight = weight;
	}

	public String toString() {
		return String.format("[class: %s, weight: %#s]", classID, weight);
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
	
}
