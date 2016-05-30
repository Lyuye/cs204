package peersim.chord;
import java.math.BigInteger;
import java.util.Comparator;

import peersim.core.Node;
public class IndexComparator implements Comparator {

	public int pid = 0;
	public BigInteger target;

	public IndexComparator(int pid, BigInteger target) {
		this.pid = pid;
		this.target = target;
	}

	public int compare(Object arg0, Object arg1) {
		BigInteger one = ((ChordProtocol) ((Node) arg0).getProtocol(pid)).chordId;
		BigInteger two = ((ChordProtocol) ((Node) arg1).getProtocol(pid)).chordId;
		BigInteger abs_one = one.subtract(target).abs();
		BigInteger abs_two = two.subtract(target).abs();
		return abs_one.compareTo(abs_two);
	}

}