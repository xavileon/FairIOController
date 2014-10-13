package cat.urv.deim.ast;

import java.util.Comparator;


public class MarginalsComparator implements Comparator<DatanodeInfo> {

	@Override
	public int compare(DatanodeInfo arg0, DatanodeInfo arg1) {
		// we put a minus because it's reversed (descending)
		return -arg0.getMarginalValue().compareTo(arg1.getMarginalValue());
	}

}
