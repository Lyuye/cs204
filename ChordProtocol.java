/**
 * 
 */
package peersim.chord;

import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.math.*;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author Andrea
 * 
 */
public class ChordProtocol implements EDProtocol {

	private static final String PAR_TRANSPORT = "transport";

	private Parameters p;

	private int[] lookupMessage; 

	public int index = 0;

	public Node predecessor;

	public Node[] fingerTable;

	public Node[] successorList;

	public BigInteger chordId;

	public int m;

	public int succLSize;

	public String prefix;

	private int next = 0;

	// campo x debug
	private int currentNode = 0;

	public int varSuccList = 0;

	public int stabilizations = 0;

	public int fails = 0;
	
	public Node[] copyList;
	
	public Node[] targetList;
	
	public int copysize;
	
	public Node[][] fullList;

	/**
	 * 
	 */
	public ChordProtocol(String prefix) {
		this.prefix = prefix;
		lookupMessage = new int[1];
		lookupMessage[0] = 0;
		p = new Parameters();
		p.tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see peersim.edsim.EDProtocol#processEvent(peersim.core.Node, int,
	 *      java.lang.Object)
	 */
	public void processEvent(Node node, int pid, Object event) {
		// processare le richieste a seconda della routing table del nodo
		int flag = 0;
		p.pid = pid;
		currentNode = node.getIndex();
		if (event.getClass() == LookUpMessage.class) {
			LookUpMessage message = (LookUpMessage) event;
			message.increaseHopCounter();
			BigInteger target = message.getTarget();
			Transport t = (Transport) node.getProtocol(p.tid);
			Node n = message.getSender();
			targetList = message.gettargetList();			
			if(targetList != null && targetList[0] == node){
				//printtargetList();
				for( int x = 0; x < copysize;x++){
					if (target == ((ChordProtocol) targetList[x].getProtocol(pid)).chordId || Arrays.binarySearch(transform(((ChordProtocol) targetList[x].getProtocol(pid)).copyList), target) >= 0 ){
						flag = 1;
						break;
					}
				}
			}
			
			if (flag == 1 || target == ((ChordProtocol) node.getProtocol(pid)).chordId || Arrays.binarySearch(transform(copyList), target) >= 0 ) {
				// mandare mess di tipo final
				t.send(node, n, new FinalMessage(message.getHopCounter()), pid);
			}
			else {

				Node dest;
				if(targetList == null){
					targetList = find_successor(target);
					dest = targetList[0];
					if (dest.isUp() == false) {
						do {
							varSuccList = 0;
							stabilize(node);
							stabilizations++;
							fixFingers();
							targetList = find_successor(target);
							dest = targetList[0];
							message.storetargetList(targetList);
							
						} while (dest.isUp() == false);
					}
				}
				else{
					fullList = new Node[copysize][copysize];
					for(int z = 0;z < copysize;z++){	
						System.arraycopy(((ChordProtocol) targetList[z].getProtocol(p.pid)).find_successor(target), 0, fullList[z], 0, copysize); 
					}
					Node[] new_targetList = mergeList(fullList,copysize,target);
					if(!Arrays.equals(targetList,new_targetList)){	
						System.arraycopy(new_targetList, 0, targetList, 0, copysize); 
						dest = targetList[0];
						message.storetargetList(targetList);
						//message.targetList = targetList;
						//((ChordProtocol) dest.getProtocol(p.pid)).targetList = find_successor(target);
						//System.arraycopy(((ChordProtocol) dest.getProtocol(p.pid)).targetList, 0, find_successor(target), 0, copysize);
						if (dest.isUp() == false) {
							do {
								varSuccList = 0;
								stabilize(node);
								stabilizations++;
								fixFingers();
								for(int z = 0;z < copysize;z++){		
									System.arraycopy(((ChordProtocol) targetList[z].getProtocol(p.pid)).find_successor(target), 0, fullList[z], 0, copysize); 	
								}
								System.arraycopy(mergeList(fullList,copysize,target), 0, targetList, 0, copysize); 
								//targetList = find_successor(target);
								dest = targetList[0];
							} while (dest.isUp() == false);
						}
					}
					else{
						dest = successorList[0];
						int count =0;
						while (count < copysize) {
							if(Arrays.binarySearch(transform(((ChordProtocol) successorList[count].getProtocol(pid)).copyList), target) >= 0) {
								t.send(node, n, new FinalMessage(message.getHopCounter()), pid);
								return;
							}
							count++;
						}
						fails++;
						return;
					}
				}

			  //if (dest.getID() == successorList[0].getID() && (target.compareTo(((ChordProtocol) dest.getProtocol(p.pid)).chordId) < 0)) {
				if (dest.getID() == successorList[0].getID() && (target.compareTo(((ChordProtocol) dest.getProtocol(p.pid)).chordId) < 0) && (target.compareTo(((ChordProtocol) node.getProtocol(p.pid)).chordId) > 0)) {
					int count =0;
					while (count < copysize) {
						if(Arrays.binarySearch(transform(((ChordProtocol) successorList[count].getProtocol(pid)).copyList), target) >= 0) {
							t.send(node, n, new FinalMessage(message.getHopCounter()), pid);
							return;
						}
						count++;
					}
					fails++;
				} else {
					t.send(message.getSender(), dest, message, pid);
				}
			}
		}
		int[] tmp;
		if (event.getClass() == FinalMessage.class) {
			FinalMessage message = (FinalMessage) event;
			tmp = new int[index + 1]; 
	        System.arraycopy(lookupMessage, 0, tmp, 0, index); 
	        lookupMessage = tmp;
		  //lookupMessage = new int[index + 1];	
			lookupMessage[index] = message.getHopCounter();	
			index++;
		} 
		//this.printcopy(); 
	}
	
	@SuppressWarnings("unchecked")
	private Node[] mergeList(Node[][] fullList, int copysize, BigInteger target){
		Node[] tempList = new Node[copysize*copysize];
		Node[] returnList = new Node[copysize];
		for(int a = 0;a < copysize;a++){
			for(int b = 0;b < copysize;b++){
				tempList[copysize*a + b] = fullList[a][b];	
			}
		}
		IndexComparator ic = new IndexComparator(p.pid, target);
		Arrays.sort(tempList, ic);
		System.arraycopy(tempList, 0, returnList, 0, copysize);
		//printList(returnList);
		return returnList;
	}
	
	public Object clone() {
		ChordProtocol cp = new ChordProtocol(prefix);
		String val = BigInteger.ZERO.toString();
		cp.chordId = new BigInteger(val);
		cp.fingerTable = new Node[m];
		cp.successorList = new Node[succLSize];
		cp.currentNode = 0;
		cp.copyList = new Node[2*copysize];
		cp.targetList = new Node[copysize];
		return cp;
	}

	public int[] getLookupMessage() {
		return lookupMessage;
	}

	public void stabilize(Node myNode) {
		try {
			Node node = ((ChordProtocol) successorList[0].getProtocol(p.pid)).predecessor;
			if (node != null) {
				if (this.chordId == ((ChordProtocol) node.getProtocol(p.pid)).chordId)
					return;
				BigInteger remoteID = ((ChordProtocol) node.getProtocol(p.pid)).chordId;
				if (idInab(chordId,remoteID, ((ChordProtocol) successorList[0]
			    //if (idInab(remoteID,chordId, ((ChordProtocol) successorList[0]
						.getProtocol(p.pid)).chordId))
					successorList[0] = node;
				((ChordProtocol) successorList[0].getProtocol(p.pid))
						.notify(myNode);
			}
			//System.out.println("error");
			updateSuccessorList();
			updatecopyList();
			
		} catch (Exception e1) {
			e1.printStackTrace();
			updateSuccessor();
		}
	}

	private void updatecopyList() throws Exception {
		try {
			System.arraycopy(successorList, 0, copyList, 0, copysize - 1);
			int t = 0;
			while(t < copysize){
				Node tempnode = predecessor;
				if (tempnode == null || tempnode.isUp() == false ){
					tempnode = ((ChordProtocol) tempnode.getProtocol(p.pid)).predecessor;
				}
				copyList[copysize + t] = tempnode;
				t++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private BigInteger[] transform(Node[] copyList){
		BigInteger[] temp = new BigInteger[copyList.length];
		for(int x = 0;x < copyList.length;x++){
			temp[x] = ((ChordProtocol) copyList[x].getProtocol(p.pid)).chordId;
		}
		return temp;
	}
	private void updateSuccessorList() throws Exception {
		try {
			while (successorList[0] == null || successorList[0].isUp() == false ) {
				
				updateSuccessor();
			}
			System.arraycopy(((ChordProtocol) successorList[0]
					.getProtocol(p.pid)).successorList, 0, successorList, 1,
					succLSize - 2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void notify(Node node) throws Exception {
		BigInteger nodeId = ((ChordProtocol) node.getProtocol(p.pid)).chordId;
		if ((predecessor == null)
				|| (idInab(nodeId, ((ChordProtocol) predecessor
						.getProtocol(p.pid)).chordId, this.chordId))) {
			predecessor = node;
		}
	}

	
	private void updateSuccessor() {
		boolean searching = true;
		while (searching) {
			//System.out.println("error");
			try {
				Node node = successorList[varSuccList];
				varSuccList++;
				successorList[0] = node;
				if (successorList[0] == null
						|| successorList[0].isUp() == false) {
					if (varSuccList >= succLSize - 1) {
						searching = false;
						varSuccList = 0;
					} else
						updateSuccessor();
				}
				//updateSuccessorList();
				System.arraycopy(((ChordProtocol) successorList[0].getProtocol(p.pid)).successorList, 0, successorList, 1, succLSize - 2);
				searching = false;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private boolean idInab(BigInteger id, BigInteger a, BigInteger b) {
		if ((a.compareTo(id) == -1) && (id.compareTo(b) == -1)) {
			return true;
		}
		return false;
	}

	public Node[] find_successor(BigInteger id) {
		Node[] succ = new Node[copysize];
		for(int x = 0;x < copysize; x++){
			succ[x] = successorList[0];
		}
		try {
			if (successorList[0] == null || successorList[0].isUp() == false) {
				updateSuccessor();
			}
			if (idInab(id, this.chordId, ((ChordProtocol) successorList[0]
					.getProtocol(p.pid)).chordId)) {
				return succ;
			} else {
				return n_closest_preceding_node(id, copysize, succ);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return succ;
	}
	private Node[] returnfingers(int i, int copysize){
		int t = 1;
		Node[] temp = new Node[copysize];
		temp[0] = fingerTable[i - 1];
		for (int y = 1; y < i; y++){			
			if(((ChordProtocol) (fingerTable[i - 1 - y].getProtocol(p.pid))).chordId.compareTo(((ChordProtocol) (fingerTable[i - 1 - y + 1].getProtocol(p.pid))).chordId) != 0){
				temp[t] = fingerTable[i - 1 - y];
				t++;
				if(t == copysize){
					break;
				}
			}
		}
		while(t < copysize){
			temp[t] = successorList[0];
			t++;
		}
		return temp;
	}
	private Node[] n_closest_preceding_node(BigInteger id, int copysize, Node[] succ) {
		for (int i = m; i > 0; i--) {
			try {
				if (fingerTable[i - 1] == null
						|| fingerTable[i - 1].isUp() == false) {
					continue;
				}
				BigInteger fingerId = ((ChordProtocol) (fingerTable[i - 1]
						.getProtocol(p.pid))).chordId;
				if ((idInab(fingerId, this.chordId, id))
						|| (id.compareTo(fingerId) == 0)) {
					return returnfingers(i, copysize);

				}
				if (fingerId.compareTo(this.chordId) == -1) {
					// sono nel caso in cui ho fatto un giro della rete
					// circolare
					if (idInab(id, fingerId, this.chordId)) {
						return returnfingers(i, copysize);
					}
				}
				if ((id.compareTo(fingerId) == -1)
						&& (id.compareTo(this.chordId) == -1)) {
					if (i == 1)
						return succ;
					BigInteger lowId = ((ChordProtocol) fingerTable[i - 2]
							.getProtocol(p.pid)).chordId;
					if (idInab(id, lowId, fingerId))
						return returnfingers(i-1, copysize);
					else if (fingerId.compareTo(this.chordId) == -1)
						continue;
					else if (fingerId.compareTo(this.chordId) == 1)
						return returnfingers(i, copysize);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (fingerTable[m - 1] == null)
			return succ;
		return succ;
	}
	
private Node closest_preceding_node(BigInteger id, int copysize) {
		
		for (int i = m; i > 0; i--) {
			try {
				if (fingerTable[i - 1] == null
						|| fingerTable[i - 1].isUp() == false) {
					continue;
				}
				BigInteger fingerId = ((ChordProtocol) (fingerTable[i - 1]
						.getProtocol(p.pid))).chordId;
				if ((idInab(fingerId, this.chordId, id))
						|| (id.compareTo(fingerId) == 0)) {
					return fingerTable[i - 1];

				}
				if (fingerId.compareTo(this.chordId) == -1) {
					// sono nel caso in cui ho fatto un giro della rete
					// circolare
					if (idInab(id, fingerId, this.chordId)) {
						return fingerTable[i - 1];
					}
				}
				if ((id.compareTo(fingerId) == -1)
						&& (id.compareTo(this.chordId) == -1)) {
					if (i == 1)
						return successorList[0];
					BigInteger lowId = ((ChordProtocol) fingerTable[i - 2]
							.getProtocol(p.pid)).chordId;
					if (idInab(id, lowId, fingerId))
						return fingerTable[i - 2];
					else if (fingerId.compareTo(this.chordId) == -1)
						continue;
					else if (fingerId.compareTo(this.chordId) == 1)
						return fingerTable[i - 1];
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (fingerTable[m - 1] == null)
			return successorList[0];
		return successorList[0];
	}

	// debug function
	private void printFingers() {
		for (int i = fingerTable.length - 1; i >= 0; i--) {
			if (fingerTable[i] == null) {
				System.out.println("Finger " + i + " is null");
				continue;
			}
			if ((((ChordProtocol) fingerTable[i].getProtocol(p.pid)).chordId)
					.compareTo(this.chordId) == 0)
				break;
			System.out
					.println("Finger["
							+ i
							+ "] = "
							+ fingerTable[i].getIndex()
							+ " chordId "
							+ ((ChordProtocol) fingerTable[i]
									.getProtocol(p.pid)).chordId);
		}
	}

	private void printList(Node[] aList) {
		for (int i = aList.length - 1; i >= 0; i--) {
			if (aList[i] == null) {
				System.out.println("aList " + i + " is null");
				continue;
			}
			if ((((ChordProtocol) aList[i].getProtocol(p.pid)).chordId)
					.compareTo(this.chordId) == 0)
				break;
			System.out
					.println("aList["
							+ i
							+ "] = "
							+ aList[i].getIndex()
							+ " chordId "
							+ ((ChordProtocol) aList[i]
									.getProtocol(p.pid)).chordId);
		}
	}
	public void fixFingers() {
		if (next >= m - 1)
			next = 0;
		if (fingerTable[next] != null && fingerTable[next].isUp()) {
			next++;
			return;
		}
		BigInteger base;
		if (next == 0)
			base = BigInteger.ONE;
		else {
			base = BigInteger.valueOf(2);
			for (int exp = 1; exp < next; exp++) {
				base = base.multiply(BigInteger.valueOf(2));
			}
		}
		BigInteger pot = this.chordId.add(base);
		BigInteger idFirst = ((ChordProtocol) Network.get(0).getProtocol(p.pid)).chordId;
		BigInteger idLast = ((ChordProtocol) Network.get(Network.size() - 1)
				.getProtocol(p.pid)).chordId;
		if (pot.compareTo(idLast) == 1) {
			pot = (pot.mod(idLast));
			if (pot.compareTo(this.chordId) != -1) {
				next++;
				return;
			}
			if (pot.compareTo(idFirst) == -1) {
				this.fingerTable[next] = Network.get(Network.size() - 1);
				next++;
				return;
			}
		}
		do {
			fingerTable[next] = ((ChordProtocol) successorList[0]
					.getProtocol(p.pid)).find_successor(pot)[0];
			pot = pot.subtract(BigInteger.ONE);
			((ChordProtocol) successorList[0].getProtocol(p.pid)).fixFingers();
		} while (fingerTable[next] == null || fingerTable[next].isUp() == false);
		next++;
	}

	/**
	 */
	public void emptyLookupMessage() {
		index = 0;
		this.lookupMessage = new int[0];
	}
}
