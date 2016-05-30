package peersim.chord;

import java.math.*;
import peersim.core.*;

public class LookUpMessage implements ChordMessage {

	private Node sender;

	private BigInteger targetId;

	private int hopCounter = -1;
	
	private Node[] targetList;

	public LookUpMessage(Node sender, BigInteger targetId, Node[] targetList) {
		this.sender = sender;
		this.targetId = targetId;
		this.targetList = targetList;
	}
	
	public Node[] gettargetList(){
		return targetList;
	}
	
	public void storetargetList(Node[] targetList){
		this.targetList = targetList;
	}
	
	public void increaseHopCounter() {
		hopCounter++;
	}

	/**
	 * @return the senderId
	 */
	public Node getSender() {
		return sender;
	}

	/**
	 * @return the target
	 */
	public BigInteger getTarget() {
		return targetId;
	}

	/**
	 * @return the hopCounter
	 */
	public int getHopCounter() {
		return hopCounter;
	}

}
