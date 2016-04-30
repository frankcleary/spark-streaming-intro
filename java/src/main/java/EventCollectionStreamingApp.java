import com.google.common.base.Optional;
import org.apache.log4j.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.*;

public class EventCollectionStreamingApp {
    private static final String HOST = "localhost";
    private static final int PORT = 9999;
    private static final String CHECKPOINT_DIR = "/tmp";
    private static final Duration BATCH_DURATION = Durations.seconds(5);

    public static void main(String[] args) {
        // Configure and initialize the SparkStreamingContext
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("EventCollectionStreamingApp");
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(conf, BATCH_DURATION);
        Logger.getRootLogger().setLevel(Level.ERROR);
        streamingContext.checkpoint(CHECKPOINT_DIR);

        // Receive streaming data from the source
        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);

        // Map lines of input data (user:event) into (user, event) pairs
        JavaPairDStream<String, String> events = lines.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String rawEvent) throws Exception {
                        String[] strings = rawEvent.split(":");
                        return new Tuple2<>(strings[0], strings[1]);
                    }
                }
        );

        // Print new events received in this batch
        events.foreachRDD(
                new Function2<JavaPairRDD<String, String>, Time, Void>() {
                    @Override
                    public Void call(JavaPairRDD<String, String> newEventsRdd, Time time)
                            throws Exception {
                            System.out.println("\n===================================");
                            System.out.println("New Events for " + time + " batch:");
                            for (Tuple2<String, String> tuple : newEventsRdd.collect()) {
                                System.out.println(tuple._1 + ": " + tuple._2);
                            }
                            return null;
                        }
                    });

        // Combine new events with a running total of events for each user.
        // userTotals holds pairs of (user, map of event to number of occurrences
        // of that event for that user)
        JavaPairDStream<String, Map<String, Long>> userTotals =
                events.updateStateByKey(
                        new Function2<List<String>, Optional<Map<String, Long>>,
                                Optional<Map<String, Long>>>() {
                    @Override
                    public Optional<Map<String, Long>> call(List<String> newEvents,
                        Optional<Map<String, Long>> oldEvents) throws Exception {
                        Map<String, Long> updateMap = oldEvents.or(new HashMap<>());
                        for (String event : newEvents) {
                            if (updateMap.containsKey(event)) {
                                updateMap.put(event, updateMap.get(event) + 1L);
                            } else {
                                updateMap.put(event, 1L);
                            }
                        }
                        return Optional.of(updateMap);
                    }
                });

        userTotals.foreachRDD(
                new Function2<JavaPairRDD<String, Map<String, Long>>, Time, Void>() {
                    @Override
                    public Void call(JavaPairRDD<String, Map<String, Long>> userTotals,
                                     Time time) throws Exception {
                        // Instead of printing this would be a good place to do
                        // something like writing the aggregation to a database
                        System.out.println("");
                        System.out.println("Per user aggregate events at " + time + ":");
                        // Consider rdd.foreach() instead of collectAsMap()
                        for (Map.Entry<String, Map<String, Long>> userData :
                                userTotals.collectAsMap().entrySet()) {
                            System.out.println(String.format("%s: %s",
                                    userData.getKey(), userData.getValue()));
                        }
                        return null;
                    }
                });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
