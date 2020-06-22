package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
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

	private boolean stop;
	private String decryptedPassword;

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.decryptedPassword = "";
	}
	
	////////////////////
	// Actor Messages //
	////////////////////


	@AllArgsConstructor
	public static class WorkerAvailableMessageToMaster implements Serializable{}

	@Data
	@AllArgsConstructor @NoArgsConstructor
	public static class DecryptedHint implements Serializable {
		private int ID;
		private String encryptedHint;
		private String decryptedHint;
	}

	@Data
	@AllArgsConstructor @NoArgsConstructor
	public static class PasswordCompleteMessage implements Serializable {
		private int ID;
		private String encryptedPassword;
		private String decryptedPassword;
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
				.match(Master.GoCrackPasswordMessage.class, this::handle)
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

		//System.out.println(this.hint);

		List<String> allPermutations = new ArrayList<>(); //Not needed unless we want to see permutations checked
		this.stop = false;
		heapPermutation(message.getHintCharacterCombination(), message.getHintCharacterCombination().length, allPermutations);
		//this.log().info("Size of permutations tried: " + allPermutations.size());

		this.master.tell(new WorkerAvailableMessageToMaster(), this.self()); //tell master it is free

		//here
		//System.out.println(this.hint);
		//System.out.println(hash(new String(message.getHintCharacterCombination())));
	}

	private void handle(Master.GoCrackPasswordMessage message) { //13. Here worker receives a password to crack
		//see how to get characters from the hints!
		//Master should send all hints (so the password object) through here so the worker can work on the password
		this.ID = message.getPassword().getID(); //Fields are obtained in this way


		String encrypted = message.getPassword().getEncryptedPassword(); //This we should change
		//System.out.println("encryptedPassword: " + encrypted);


		String[] hints = message.getPassword().getHintsDecryptedArray().clone();
		char[] alphabet = message.getPassword().getPossibleCharacters().clone();


		char[] set = getMissingCharactersofHint(hints, alphabet);
		int k = message.getPassword().getPasswordLength();
		int n = set.length;

		printAllKLengthRec(set, "", n,k,encrypted);
		if(!this.decryptedPassword.equals("")) {
			this.master.tell(new PasswordCompleteMessage(this.ID, encrypted, this.decryptedPassword), this.self());
			return;
		}
		this.log().info("No password found");
		this.master.tell(new PasswordCompleteMessage(this.ID, encrypted, this.decryptedPassword), this.self());
	}

	private char[] getMissingCharactersofHint (String[] hints, char[] alphabet){

		List<Character> alphabetList = new ArrayList<Character>();
		for (char c : alphabet) {
			alphabetList.add(c);
		}


		for (String hintString : hints) {
			//System.out.println(hintString);
			List<Character> newAlphabet =  new ArrayList<Character>();
			String tempHint = hintString; // HJKGDEFBIC
			for (int i = 0; i < tempHint.length(); i++) {
				//System.out.println(tempHint.charAt(i));
				char hintChar =tempHint.charAt(i); //H J K
				for (int k = 0; k < alphabetList.size(); k++) {
					char alphChar = alphabetList.get(k);   //A
					//System.out.println("Alphabet letter: " + alphChar);
					if(alphChar == hintChar) {
						//System.out.println("YES!");
						//if the hintchar is not in the alphabet
						if(alphabetList.contains(hintChar)) {
							//System.out.println("MORE YES");
							newAlphabet.add(hintChar);
						}
					}
				}
			}
			alphabetList = newAlphabet;
		}

		String returnstring = "";
		for (int j = 0; j < alphabetList.size(); j++) {
			//System.out.println(alphabetList.get(j));
			returnstring = returnstring +alphabetList.get(j);
		}
		//System.out.println(returnstring);
		char[] returnCharArray =returnstring.toCharArray();
		return returnCharArray;

	}

	// Generating all possible strings of length k
	// strings are hashed
	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/

	 void printAllKLengthRec(char[] set, String prefix, int n, int k, String encrypted)
	{
		String password = encrypted;

		// Base case: k is 0,
		// print prefix
		if (k == 0)
		{
			String curr_hashed = hash(prefix);
			if (curr_hashed.equals(password)){
				this.decryptedPassword = prefix;
				this.log().info("Found password for ID  " + this.ID + ": " + this.decryptedPassword);
			}
			return;
		}

		// One by one add all characters
		// from set and recursively
		// call for k equals to k-1
		for (int i = 0; i < n; ++i)
		{

			// Next character of input added
			String newPrefix = prefix + set[i];

			// k is decreased, because
			// we have added a new character
			printAllKLengthRec(set, newPrefix,
					n, k - 1, encrypted);
		}
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
		if (this.stop){
			return;
		}

		if (size == 1)
		{
			l.add(new String(a));//add permutation to a list but instead we can hash it here
			//Hash permutation
			String permutationHash = hash(new String(a));
			if(this.hint.equals(permutationHash)){
				this.stop = true;
				this.log().info("Hint decrypted");
				this.master.tell(new DecryptedHint(this.ID, this.hint, new String(a)), this.self());

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