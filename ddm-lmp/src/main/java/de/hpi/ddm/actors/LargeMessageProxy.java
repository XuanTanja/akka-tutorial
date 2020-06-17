package de.hpi.ddm.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import akka.japi.function.Creator;
import java.util.Iterator;
import java.util.stream.IntStream;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	private ActorRef MasterField_worker_receiver_url;

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	//https://projectlombok.org/features/constructor
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef message_receiver_worker_master;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class MasterInquiry implements Serializable { //Send message asking if worker can receive a message
		private ActorRef master; //master
		private ActorRef master_largeMessageProxy_url; //largeMessageProxy from master
		private ActorRef receiver_worker; //worker
	}


	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
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



	private byte[] messageOutgoing; //Whole message that is going to be sent
	//private byte[] messageIngoing = new byte[0];
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////


	private class Creator2 implements Creator {

		Iterator2 iterator = new Iterator2();

		@Override
		public Object create() throws Exception {
			return iterator;
		}
	}

	private class Iterator2 implements Iterator<Byte[]> {

		private int i = 0;
		//this is more or less arbitrary, but gives good results TODO: change this number and variable names??
		private int size = 200000;

		@Override
		public boolean hasNext() {
			return messageOutgoing.length > i * size;
		}

		@Override
		public Byte[] next() {
			int a = i * size; //chunk of data starting point
			int o = a + size; //data chunk ending point
			if (o > messageOutgoing.length) { //if
				o = messageOutgoing.length;
			}
			i++;
			return IntStream.range(a, o).mapToObj(k -> Byte.valueOf(messageOutgoing[k])).toArray(Byte[]::new);
		}
	}



	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(MasterInquiry.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	//This handler is from the master/largeMessageProxy!
	private void handle(LargeMessage<?> message) { // 7. Master sends (sender) a LargeMessage to the master/largeMessageProxy (and get received by this handler). Message contains message_info and the url to the worker (receiver) in the LargeMessage message
		ActorRef receiver = message.getMessage_receiver_worker_master(); //thi is the worker url which was sent in the message LargeMessage
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME)); //Here we get worker url

		//Starting kryo serialization (create Source -> create sink -> materialize)
		//https://doc.akka.io/docs/akka/2.5.22/stream/stream-quickstart.html
		//https://github.com/twitter/chill
		//https://en.wikibooks.org/wiki/Java_Akka_Streams/Sources
		this.messageOutgoing = KryoPoolSingleton.get().toBytesWithClass(message.getMessage()); //converting data into bytes and saving to array

		//System.out.println("IMPORTANT Large Message Proxy /Master: " + this.sender()+this.self()+this.MasterField_worker_receiver_url);
		receiverProxy.tell(new MasterInquiry(this.sender(), this.self(), this.MasterField_worker_receiver_url), this.self()); //**!Send from worker to master (this.sender() is the master)
		//TODO: find out where the documentation says we should use ack and confirmation message for streams****

		//TODO: MAYBE we need this????
		//Materializer from actor context
		//final Materializer materializer = ActorMaterializer.create(this.context());
		//Creator2 creator = new Creator2();
		//Source<Creator2, NotUsed> source = Source.fromIterator(creator);



		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming. OUR CHOICE
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
	}

	private void handle(MasterInquiry message) { //8. Master/largeMessageProxy sent a MasterInquiry message asking for (permission?) to send large message
		//TODO: refactor (not working yet)
		this.MasterField_worker_receiver_url = message.getReceiver_worker();
		this.master_largeMessageProxy = message.getMaster();
		message.getMaster_largeMessageProxy_url().tell(new ConfigurationMessage(this.self()), this.self()); //this.self is the LargeMessageProxy
	}



	private void handle(BytesMessage<?> message) {
	// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.

	//Receive message from master ->   handle(LargeMessage<?> message)
	//final Source<Byte, NotUsed> messageStream = source.scan(BigInteger.ONE, (acc, next) -> acc.multiply(BigInteger.valueOf(next)));

	//Deserializelike this:
	//Object deserObj = kryo.fromBytes(myObj);

	message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}
