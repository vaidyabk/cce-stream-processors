package com.vcc.cce.streams.example;

import java.time.Duration;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Use-case: 
 * The GPE-Ingress component receives price-update events from GPE. These events are at per-country-per-model level.
 * After retrieving the price document from GPE (per-country-per-model), the next steps are to compute the virtual price list and send it to price-service, 
 * where more heavy computations happen.
 * 
 * The trouble is, virtual price list is at country level. When multiple models' (for the same country) price document gets updated in a short burst, 
 * price-service is burdened with several heavy re-computations unnecessarily.
 * 
 * If there is a way to aggregate all price-update events by country over a time window, then the gpe-ingress component could be engineered to send ONE virtual price list 
 * that contains updated price values received over several events (for different models).
 * 
 * Also a desired behavior: Price updates events for several countries may be (and should be) processed in parallel.
 * 
 * It was examined if there was a possibility to accumulate a set of records in Kafka stream before triggering a process with batched input of the record-set.
 * 
 * Streams API - with TimeWindow aggregation fits this use case. 
 * 
 * +++ NOTE ++++ Kafka streams and processor APIs are available ONLY in Java as of this writing.
 * 
 * However, a blocker was encountered with this approach - it works ONLY if there is steady stream of events due to dependence on "stream-time":
	https://stackoverflow.com/questions/54222594/kafka-stream-suppress-session-windowed-aggregation
	https://stackoverflow.com/questions/60822669/kafka-sessionwindow-with-suppress-only-sends-final-event-when-there-is-a-steady

 * If a burst of price-update events happen for the Sweden, followed by no activity on any other country for extended times, then aggregate operation for Sweden 
 * would NOT be triggered in a timely way. Markets shall complain about stale price data being presented by CCE service.
 * 
 * This example implements a workaround streams-transformation topology suggested in the links above. This solution will add one more microservice to the ecosystem and becomes viable if 
 * there are additional stream processing requirements in CCE. Spring is also an option for such a microservice.
 * 
 * Change: when events are sent to topic "external-price-notification" - use "country" as the key.
 * 
 * @author BKUMARVA
 *
 */
public class PriceUpdatesEventsBatcher {

	public static void main(String args[]) throws Exception {
		consume();
	}

	public static void consume() throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "price-updates-batcher");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "centos.com:9092");
		props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/TBK/kafkaStrTmp");

		String topicName = "external-price-notification"; 	// per-country-per-model events arrive here
		String stateStoreName = "windowed_txn"; 			// State store
		long inactivityDurationMillis = 300000L; 			// duration of time to wait before batching update events (after the arrival "last" event)
		long scanIntervalMillis = 10000L; 					// frequency at which the aggregator should check of windows that are ready to be batched

		StoreBuilder<KeyValueStore<String, Long>> keyValueStoreBuilder = Stores
				.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName), Serdes.String(), Serdes.Long());

		StreamsBuilder builder = new StreamsBuilder();

		builder.addStateStore(keyValueStoreBuilder); // Kafka backs this state-store with a topic. Name of that topic shall be "$topicName-$stateStoreName-changelog"

		builder.stream(topicName, Consumed.with(Serdes.String(), Serdes.String()))
				.transform(() -> new SessionWindowAggregator(stateStoreName, inactivityDurationMillis, scanIntervalMillis), stateStoreName)
				.foreach((country, lastUpdateTs) -> 
					System.out.println("Aggregated Event:: raised at:[" + new Date()+ "] Country:[" + country + "] LastEventAt: [" + new Date(lastUpdateTs) + "] "));

		// instead of for-each, this could be streamed to another topic - which then could be action-ized
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// see comments on this in the method below
		streams.cleanUp();

		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		Thread.sleep(3000000);
		streams.close();
	}
}

class SessionWindowAggregator implements Transformer<String, String, KeyValue<String, Long>> {
	private ProcessorContext context;
	private Cancellable cancellable;

	private KeyValueStore<String, Long> kvStore; //stores country and timestamp of arrival of last event for that country

	private long inactivityMillis;
	private String stateStoreName;
	private long scanIntervalMillis;

	public SessionWindowAggregator(String stateStoreName, long inactivityMillis, long scanIntervalMillis) {
		this.stateStoreName = stateStoreName;
		this.inactivityMillis = inactivityMillis;
		this.scanIntervalMillis = scanIntervalMillis;
	}

	@Override
	public void init(ProcessorContext context) {
		this.context = context;
		kvStore = (KeyValueStore) context.getStateStore(stateStoreName);
		cancellable = context.schedule(Duration.ofMillis(scanIntervalMillis), PunctuationType.WALL_CLOCK_TIME,
				timestamp -> flushOldWindow());
	}

	@Override
	public KeyValue<String, Long> transform(String key, String value) { // Triggered for each price-update event
		kvStore.put(key, System.currentTimeMillis()); 			// update the country entry with timestamp of arrival of last event. which is "now".
		flushOldWindow(); 										// optional; alternately, flush may be triggered only by punctuator
		return null; 											// suppress
	}

	private void flushOldWindow() { // Trigger by punctuator cron
		// logic to check for old windows in kvStore then flush
		KeyValueIterator<String, Long> it = kvStore.all();
		long tsCriteria = System.currentTimeMillis() - inactivityMillis; // latest record older than "inactivity" duration
		while (it.hasNext()) { // assumes # of countries is not too high
			KeyValue<String, Long> entry = it.next();
			if (entry.value < tsCriteria) {
				// forward (or unsuppressed) your suppressed records downstream using ProcessorContext.forward(key, value)
				this.context.forward(entry.key, entry.value);
				kvStore.delete(entry.key);
			}
		}
		it.close();
	}

	@Override
	public void close() {
		cancellable.cancel();// cancel punctuate
	}
}