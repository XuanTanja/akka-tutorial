package de.hpi.ddm.actors;

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

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster = Cluster.get(this.context().system());
	private final ActorRef largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	
	private long registrationTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() { //
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class); // 2. here worker is subscribing to change notifications of the cluster. This is the first message sent from worker to cluster on being created
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
				.match(Object.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) { // 3. CurrentClusterState is sent as first message from cluster to subscriber after subscriber subscribed on preStart
		message.getMembers().forEach(member -> { // Iterate through all members that the CurrentClusterState was sent to (so all cluster members)
			if (member.status().equals(MemberStatus.up())) // If the status is up (so the node is responsive)
				this.register(member); // 4. this register function is evoked
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member()); // 4. members who send memberUp message will also evoke register function (in case CurrentClusterState didnt get recieved)
	}

	private void register(Member member) { //If there is no masterSystem reference and the member has role MASTER_ROLE, then set this reference as this.masterSystem
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			this.registrationTime = System.currentTimeMillis();
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self()); // 5. Here the worker communicates with the master for the first time, which is inside of masterSystem
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(Object message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().info("Data received in " + transmissionTime + " ms.");
	}
}