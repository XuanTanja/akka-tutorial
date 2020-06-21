package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.dsl.Creators;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class DecryptedHint implements Serializable {
		private int ID;
		private String encryptedHint;
		private String decryptedHint;
	}

	/////////////////
	// Actor State //
	/////////////////

	//Actor variables
	private Member masterSystem;
	private final Cluster cluster;
	private ActorRef master;
	private String hint;
	private int ID;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class); //2. subscribe to cluster
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(Master.DecryptHintMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) { //3. receive this message from MasterSystem Cluster (registerOnMemberUp)
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member); //register all workers to the master
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) { //only used to register the workers (by sending message to the master and having master watch them)
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self()); //4.Send message to Master
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(Master.DecryptHintMessage message) { //10. Worker receives decryptHintMessage message and starts  decrypting
		//System.out.println("Got message!!!");
		this.master = this.sender();
		this.ID = message.getID();
		this.hint = message.getHint();
		this.log().info("Started decrypting hint");
		List<String> allPermutations = new ArrayList<>(); //Not needed unless we want to see permutations checked
		heapPermutation(message.getHintCharacterCombination(), message.getHintCharacterCombination().length, allPermutations);

		System.out.println(this.hint);
		System.out.println(hash(new String(message.getHintCharacterCombination())));
		System.out.println("Size of permutations tried: " + allPermutations.size());
	}


	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	//This implementation is inefficient because it calculates a permutation for each hint (instead of grouping all hints per permutation to calculate a permutation only once)
	private void heapPermutation(char[] a, int size, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
		{
			l.add(new String(a));//add permutation to a list but instead we can hash it here
			String permutationHash = hash(new String(a));
			//Compare this peremutationHash with the hint

			if(this.hint.equals(permutationHash)){
				//Send message
				System.out.println("Decrypted!!");
				this.master.tell(new DecryptedHint(this.ID, this.hint, new String(a)), this.self());
				//return;
			}
		}


		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}