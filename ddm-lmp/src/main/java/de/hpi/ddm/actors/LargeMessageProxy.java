package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.*;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
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
		private ActorRef receiver;
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

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class RequestMessage implements Serializable {
		private ActorRef master; //master
		private ActorRef sender; //largeMessageProxy from master
		private ActorRef receiver; //worker
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
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	//This handler is from the master/largeMessageProxy!
	private void handle(LargeMessage<?> message) { // 7. Master sends (sender) a LargeMessage to the master/largeMessageProxy (and get received by this handler). Message contains message_info and the url to the worker (receiver) in the LargeMessage message
		ActorRef receiver = message.getReceiver(); //thi is the worker url which was sent in the message LargeMessage
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		//Starting kryo serialization (create Source -> create sink -> materialize)
		//https://doc.akka.io/docs/akka/2.5.22/stream/stream-quickstart.html
		//https://github.com/twitter/chill
		//https://en.wikibooks.org/wiki/Java_Akka_Streams/Sources
		this.messageOutgoing = KryoPoolSingleton.get().toBytesWithClass(message.getMessage()); //converting data into bytes and saving to array

		//System.out.println("IMPORTANT: " + this.sender()+this.self()+this.receiver);
		//receiverProxy.tell(new RequestMessage(this.sender(), this.self(), this.receiver), this.self()); //**!Send from worker to master (this.sender() is the master)


		//Materializer from actor context
		final Materializer materializer = ActorMaterializer.create(this.context());

		Creator2 creator = new Creator2(); //TODO
		Source<Creator2, NotUsed> source = Source.fromIterator(creator);

		/*
		Sink sink = Sink.actorRefWithAck(
				message.getSender(),
				new StreamInitializedMessage(),
				Ack.INSTANCE,
				new StreamCompletedMessage(),
				err -> new StreamFailureMessage(err)
		);

		 */

		//source.runWith()


		//TODO pass creator to source and run it to generate the numbers from iterator

		/*
		Iterable<Integer> numbersOneToTen = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		Source<Integer, NotUsed> source = Source.from(numbersOneToTen);
		Sink<Integer, CompletionStage<Done>> foreach = Sink.foreach(System.out::println);
		CompletionStage<Done> futureDone = source.runForeach(System.out::println, materializer);
		futureDone.toCompletableFuture().join();

		//Graph example
		Iterable<Integer> numbersOneToTen = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		Source<Integer, NotUsed> source = Source.from(numbersOneToTen);
		Sink<Integer, CompletionStage<Done>> foreach = Sink.foreach(System.out::println);
		RunnableGraph<CompletionStage<Done>> graph = source.toMat(foreach, Keep.right());
		CompletionStage<Done> futureDone = graph.run(materializer);
		futureDone.toCompletableFuture().join();
		 */


		/*
		Sink sink = Sink.actorRefWithAck(
				message.getSender(),
				new StreamInitializedMessage(),
				Ack.INSTANCE,
				new StreamCompletedMessage(),
				err -> new StreamFailureMessage(err)
		);
		Source.fromIterator(creator).runWith(sink, ActorMaterializer.create(this.context()));
		 */

		//Create iterator to run and send message
		//Source


		//Like this : KryoPoolSingleton.get().toBytesWithClass(message.getMessage());



		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
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
