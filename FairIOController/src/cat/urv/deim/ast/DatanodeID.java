package cat.urv.deim.ast;

public class DatanodeID implements Comparable<DatanodeID> {

	private static int globalID = 0;
	
	public int id;
	
	public DatanodeID() {
		this.id = globalID;
		globalID++;
	}
	
	public String toString() {
		return String.valueOf(id);
	}
	
	@Override
	public int compareTo(DatanodeID arg0) {
		if (this.id < arg0.id) return -1;
		else if (this.id == arg0.id) return 0;
		else return 1;
	}

}
