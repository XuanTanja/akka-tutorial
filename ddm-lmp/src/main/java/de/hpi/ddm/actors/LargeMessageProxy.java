package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.stream.IntStream;
import java.util.Iterator;
import java.util.List;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.japi.function.Creator;
import akka.stream.ActorMaterializer;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	private ActorRef masterField_worker_receiver_url; //receiver
	private ActorRef master_largeMessageProxy; //sender
	private List<Byte> requestIncoming = new ArrayList<Byte>();
	private byte[] messageOutgoing = new byte[0];  //Whole message that is going to be sent

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	@NoArgsConstructor
	private static class StreamCompletedMessage implements Serializable{}

	@Data
	@NoArgsConstructor
	private static class StreamInitializedMessage implements Serializable {}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class StreamFailureMessage implements Serializable {
		private Throwable cause;
	}

	//https://projectlombok.org/features/constructor
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = -6744533868121116774L;
		private T message;
		private ActorRef message_receiver_worker_master; //receiver
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class MasterInquiryMessage implements Serializable { //Send message asking if worker can receive a message
		private ActorRef master; //master
		private ActorRef master_largeMessageProxy_url; //largeMessageProxy from master ~sender
		private ActorRef receiver_worker; //worker ~receiver
	}


	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 880408188819632523L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}


	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private  static class ConfigurationMessage implements Serializable {
		private ActorRef sender;
	}



	/////////////////
	// Actor State //
	/////////////////

	enum Ack {
		INSTANCE
	}


	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////


	private class Creator2 implements Creator {

		Iterator2 iterator = new Iterator2();

		@Override
		public Object create()  {
			return iterator;
		}
	}

	private class Iterator2 implements Iterator<Byte[]> {

		private int temp = 0;
		private final int iteratorsize = 150000;

		@Override
		public boolean hasNext() {
			return messageOutgoing.length > temp * iteratorsize;
		}

		@Override
		public Byte[] next() {
			int chunkstart = temp * iteratorsize; //chunk of data starting point
			int chunkend = chunkstart + iteratorsize; //data chunk ending point
			if (chunkend > messageOutgoing.length) {
				chunkend = messageOutgoing.length;
			}
			temp++;
			return IntStream.range(chunkstart, chunkend).mapToObj(k -> Byte.valueOf(messageOutgoing[k])).toArray(Byte[]::new);
		}
	}


	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(MasterInquiryMessage.class, this::handle)
				.match(ConfigurationMessage.class, this::handle)
				.match(StreamInitializedMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(Byte[].class, this::handle)
				.match(StreamCompletedMessage.class, this::handle)
				.match(StreamFailureMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	////////////////////
	//  Event Handler //
	////////////////////


	//This handler is from the master/largeMessageProxy!
	private void handle(LargeMessage<?> message) { // 7. Master sends (sender) a LargeMessage to the master/largeMessageProxy (and get received by this handler). Message contains message_info and the url to the worker (receiver) in the LargeMessage message
		this.masterField_worker_receiver_url = message.getMessage_receiver_worker_master(); //this is the worker url which was sent in the message LargeMessage
		ActorSelection receiverProxy = this.context().actorSelection(masterField_worker_receiver_url.path().child(DEFAULT_NAME)); //Here we get worker url



		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming. OUR CHOICE
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...

		//Starting Kryo serialization, then (create Source -> create sink -> materialize)
		//Serialization Kryo documentation: https://github.com/twitter/chill
		//Official Streaming documentation: https://doc.akka.io/docs/akka/2.5.22/stream/stream-quickstart.html
		//Streaming documentation: https://en.wikibooks.org/wiki/Java_Akka_Streams/Sources
		this.messageOutgoing = KryoPoolSingleton.get().toBytesWithClass(message.getMessage()); //Serialization: converting data into bytes and saving to array

		//Send from worker to master: sender = master, receiver = worker
		receiverProxy.tell(new MasterInquiryMessage(this.sender(), this.self(), this.masterField_worker_receiver_url), this.self());

	}

	private void handle(MasterInquiryMessage masterInquiryMessage) { //8. Master/largeMessageProxy sent a MasterInquiry message asking for (permission?) to send large message
		this.masterField_worker_receiver_url = masterInquiryMessage.getReceiver_worker();
		this.master_largeMessageProxy = masterInquiryMessage.getMaster();
		masterInquiryMessage.getMaster_largeMessageProxy_url().tell(new ConfigurationMessage(this.self()), this.self()); //this.self is the LargeMessageProxy
	}

	private void handle(ConfigurationMessage configurationMessage) {
		//see also: https://en.wikibooks.org/wiki/Java_Akka_Streams/Sources

		Creator2 creator = new Creator2();


		Sink sink = Sink.actorRefWithAck(
				configurationMessage.getSender(),
				new StreamInitializedMessage(),
				Ack.INSTANCE,
				new StreamCompletedMessage(),
				err -> new StreamFailureMessage(err)
		);
		Source.fromIterator(creator).runWith(sink, ActorMaterializer.create(this.context()));

	}

	private void handle(StreamInitializedMessage streamInitializedMessage) {
		sender().tell(Ack.INSTANCE, self());
	}

	private void handle(BytesMessage<byte[]> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.

		message.getReceiver().tell(KryoPoolSingleton.get().fromBytes(message.getBytes()), message.getSender()); //Deserialization: de-converting bytes
	}

	private void handle(Byte[] bytes) {
		// Rebuilding the requestIncoming
		for(Byte temp : bytes){
			this.requestIncoming.add(temp);
		}
		sender().tell(Ack.INSTANCE, self());
	}

	private void handle(StreamCompletedMessage streamCompletedMessage) {

		byte[] bytes = new byte[this.requestIncoming.size()];
		int temp = 0;
		while (temp < this.requestIncoming.size()) {
			bytes[temp] = this.requestIncoming.get(temp);
			temp++;
		}
		this.masterField_worker_receiver_url.tell(KryoPoolSingleton.get().fromBytes(bytes), this.master_largeMessageProxy); //Deserialization: de-converting bytes
	}

	private void handle(StreamFailureMessage streamFailureMessage) {
		this.log().error(streamFailureMessage.toString());
	}

}
