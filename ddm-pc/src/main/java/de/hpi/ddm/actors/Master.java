package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.*;

public class Master extends AbstractLoggingActor {

	/////////////////
	// Actor State //
	/////////////////

	//Master Variables
	public static final String DEFAULT_NAME = "master";
	private int passwordLength = -1; //-1 when no batch has been read
	private char[] charactersInPassword;
	private List<char[]> possibleCombinationsForHintsList;
	HashMap<Integer, Password> fileIndex_PasswordHashMap; //Hashmap with all fields from password file

	//http://tutorials.jenkov.com/java-collections/queue.html
	private Queue<DecryptHintMessage> hintCrackingQueue;
	private  Queue<DecryptPassword> passwordCrackingQueue;

	//Actor references
	private final ActorRef reader;
	private final ActorRef collector;

	private final List<ActorRef> workers;
	private List<Boolean> workerOccupied;

	private long startTime;

	////////////////////////
	// Actor Construction //
	////////////////////////
	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.workerOccupied = new ArrayList<>();
		this.possibleCombinationsForHintsList = new ArrayList<char[]>();
		this.passwordLength = -1;
		this.fileIndex_PasswordHashMap = new HashMap<Integer, Password>();

		this.hintCrackingQueue = new LinkedList<DecryptHintMessage>();
		this.passwordCrackingQueue = new LinkedList<DecryptPassword>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Getter @Setter @ToString @AllArgsConstructor
	public static class DecryptHintMessage implements Serializable {
		private int ID;
		private String hint;
		private char[] hintCharacterCombination; //possible characters in the hint
	}

	@Getter @Setter @ToString @AllArgsConstructor
	public static class DecryptPassword implements Serializable {
		private int ID;
		private String password;
		private String[] hints;
		private char[] hintCharacterCombination; //possible characters in the hint
		private boolean messageAlreadySentToWorker = false;
		private int length;
	}
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RegistrationMessage.class, this::handle)
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Worker.DecryptedHint.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}


	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		this.reader.tell(new Reader.ReadMessage(), this.self()); //7. Master tells reader message:  ReadMessage
	}
	
	protected void handle(BatchMessage message) { //HERE messages arrive in batches sent from Reader
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// Implement the processing of the data for the concrete assignment. /////////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		//If nothing is in the message
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}

		if(passwordLength == -1){ //-1 means that it is the first batch
			this.passwordLength = Integer.parseInt(message.getLines().get(0)[3]); //assign password length
			this.charactersInPassword = message.getLines().get(0)[2].toCharArray();//assign possible characters in password
			//Assign combinations for hints (each entry has 10 elements from the 11 characters, so 11 entries in total)
			//System.out.println("Length before creating permutation: " + possibleCombinationsForHintsList.size());
			getCharacterCombinations(this.charactersInPassword, this.passwordLength, possibleCombinationsForHintsList);//make function for combinations
			//System.out.println("Length after creating permutation: " + possibleCombinationsForHintsList.size());
		}

		String[] passwordHints;
		for (String[] messageLine : message.getLines()) {
			//System.out.println(Arrays.toString(messageLine)); //Print message
			//System.out.println(messageLine[4]);
			int fileIndex = Integer.parseInt(messageLine[0]);
			passwordHints = new String[messageLine.length-4];
			for (int i = 4; i < messageLine.length; i++) {
				passwordHints[i-4] = messageLine[i];
			}
			Password password = new Password(fileIndex, messageLine[1], messageLine[4], passwordHints);
			//System.out.println(password);
			fileIndex_PasswordHashMap.put(password.getID(), password); //adding password to hashmap
			for (int i = 0; i < password.getHintsEncryptedArray().length; i++) {
				for (int j = 0; j < this.possibleCombinationsForHintsList.size(); j++) {
					//add to queue
					this.hintCrackingQueue.add(new DecryptHintMessage(password.getID(), password.getHintsEncryptedArray()[i], this.possibleCombinationsForHintsList.get(j)));//Add hint cracking task to hintCrackingQueue
				}
			}
			//System.out.println("hintCrackingQueue length: " + hintCrackingQueue.size());
			this.sendDecryptHintMessage(); //9.Send messages from hintCrackingQueue to Workers that are free (workerOccupied)
		}
		//System.out.println(passwordFileIndexHashMap.size());
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		//this.reader.tell(new Reader.ReadMessage(), this.self()); //tell reader to send more batches of messages
	}

	protected void sendDecryptHintMessage(){
		for (int i = 0; i < workerOccupied.size(); i++) {
			if (this.workerOccupied.get(i)==false){
				DecryptHintMessage messageToSend = this.hintCrackingQueue.remove(); //.poll para ver si tiene elemento primero
				this.workers.get(i).tell(messageToSend, this.self());
				this.workerOccupied.set(i, true); //Set occupied
			}
		}
	}
	//hacer para decrypt message


	private void handle(Worker.DecryptedHint message) { //11. Master receives hint decrypted from worker. With this we know worker is free so we can send it more messages
		//save the decrypted hint and send more work
		//first try to send work for password then for hints
		int ID = message.getID();
		String encrypted = message.getEncryptedHint();
		String decrypted = message.getDecryptedHint();
		this.sender();
		for (int i = 0; i < workers.size(); i++) {
			if(this.sender().equals(workers.get(i))){
				System.out.println("SIII");
				this.workerOccupied.set(i, false); //Set occupied
			}
		}

		//check if all hints are cracked
	}

	private boolean checkDecryptedHints(int ID){

		return false;
	}


	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) { //5. Master receives message and starts watching it
		this.context().watch(this.sender());
		this.workers.add(this.sender()); //add worker to the worker arraylist from the Master
		this.workerOccupied.add(false);
		//System.out.println("workerOccupied size: " + workerOccupied);
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		//TODO: remove these workers from the arrays below
		//this.workers
		//this.workerOccupied

//		this.log().info("Unregistered {}", message.getActor());
	}

	//Character combinations for password hints
	//https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	//https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	private void getCharacterCombinations(char[] possibleCharacters, int passwordLength, List<char[]> dataList) {
		char[] combination = new char[possibleCharacters.length - 1];
		for (int i = 0; i < possibleCharacters.length; i++) {
			int combination_index = 0;
			for (int j = 0; j < possibleCharacters.length; j++) {
				if (j != i) {
					combination[combination_index++] = possibleCharacters[j];
				}
			}
			if (combination.length == passwordLength) {

				dataList.add(combination.clone());
			} else {
				getCharacterCombinations(combination, passwordLength, dataList);
			}
		}
	}


 	//https://projectlombok.org/features/Data
	@Getter @Setter @ToString
	protected class Password implements Serializable, Cloneable{
		private int ID;
		private String name;
		private String encryptedPassword;
		private String decryptedPassword;
		private String[] hintsEncryptedArray;
		private String[] hintsDecryptedArray;
		private boolean crackedPassword;

		public Password(int ID, String name, String encryptedPassword, String[] hintsEncryptedArray){
			this.ID = ID;
			this.name = name;
			this.encryptedPassword = encryptedPassword;
			this.decryptedPassword = "";
			this.hintsEncryptedArray = hintsEncryptedArray.clone();
			this.hintsDecryptedArray = new String[this.hintsEncryptedArray.length];
			Arrays.fill(this.hintsDecryptedArray, "");
			crackedPassword = false;
		}

		public Object clone(){
			try {
				return super.clone();
			} catch (CloneNotSupportedException e){
				return this;
			}
		}
	}



}
