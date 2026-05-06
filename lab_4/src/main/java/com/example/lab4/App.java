package com.example.lab4;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class App {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APPLICATION_ID", "lab4-kafka-streams-app"));
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("BOOTSTRAP_SERVERS", "broker1:29092,broker2:29093"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        properties.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        String inputTopic = env("INPUT_TOPIC", "Topic1");
        String outputAverageTopic = env("OUTPUT_AVG_TOPIC", "trip-duration-avg-by-day");
        String outputCountTopic = env("OUTPUT_COUNT_TOPIC", "trip-count-by-day");
        String outputTopStartTopic = env("OUTPUT_TOP_START_TOPIC", "top-start-station-by-day");
        String outputTopThreeTopic = env("OUTPUT_TOP3_TOPIC", "top-3-stations-by-day");

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<TripEvent> tripSerde = new JsonSerde<>(TripEvent.class);
        JsonSerde<AverageAccumulator> averageAccumulatorSerde = new JsonSerde<>(AverageAccumulator.class);
        JsonSerde<AverageDurationResult> averageResultSerde = new JsonSerde<>(AverageDurationResult.class);
        JsonSerde<DailyTripCountResult> tripCountResultSerde = new JsonSerde<>(DailyTripCountResult.class);
        JsonSerde<MostPopularStationAccumulator> popularStationAccumulatorSerde = new JsonSerde<>(MostPopularStationAccumulator.class);
        JsonSerde<MostPopularStationResult> popularStationResultSerde = new JsonSerde<>(MostPopularStationResult.class);
        JsonSerde<TopStationsAccumulator> topStationsAccumulatorSerde = new JsonSerde<>(TopStationsAccumulator.class);
        JsonSerde<TopStationsResult> topStationsResultSerde = new JsonSerde<>(TopStationsResult.class);

        KStream<String, TripEvent> tripsByDay = builder.stream(inputTopic, Consumed.with(Serdes.String(), tripSerde))
                .filter((key, trip) -> trip != null && trip.getStartTime() != null && !trip.getStartTime().isBlank())
                .selectKey((key, trip) -> extractTripDate(trip.getStartTime()));

        KTable<String, AverageAccumulator> averageDurationByDay = tripsByDay
                .groupByKey(Grouped.with(Serdes.String(), tripSerde))
                .aggregate(
                        AverageAccumulator::new,
                        (day, trip, aggregate) -> aggregate.addTrip(trip.getTripDurationSeconds()),
                        Materialized.with(Serdes.String(), averageAccumulatorSerde)
                );

        averageDurationByDay
                .toStream()
                .mapValues((day, aggregate) -> aggregate.toResult(day))
                .to(outputAverageTopic, Produced.with(Serdes.String(), averageResultSerde));

        tripsByDay
                .groupByKey(Grouped.with(Serdes.String(), tripSerde))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .mapValues((day, count) -> new DailyTripCountResult(day, count))
                .to(outputCountTopic, Produced.with(Serdes.String(), tripCountResultSerde));

        KTable<String, MostPopularStationAccumulator> mostPopularStartStationByDay = tripsByDay
                .groupByKey(Grouped.with(Serdes.String(), tripSerde))
                .aggregate(
                        MostPopularStationAccumulator::new,
                        (day, trip, aggregate) -> aggregate.addStation(trip.getFromStationName()),
                        Materialized.with(Serdes.String(), popularStationAccumulatorSerde)
                );

        mostPopularStartStationByDay
                .toStream()
                .mapValues((day, aggregate) -> aggregate.toResult(day))
                .to(outputTopStartTopic, Produced.with(Serdes.String(), popularStationResultSerde));

        KTable<String, TopStationsAccumulator> topStationsByDay = tripsByDay
                .groupByKey(Grouped.with(Serdes.String(), tripSerde))
                .aggregate(
                        TopStationsAccumulator::new,
                        (day, trip, aggregate) -> aggregate.addTrip(trip.getFromStationName(), trip.getToStationName()),
                        Materialized.with(Serdes.String(), topStationsAccumulatorSerde)
                );

        topStationsByDay
                .toStream()
                .mapValues((day, aggregate) -> aggregate.toResult(day, 3))
                .to(outputTopThreeTopic, Produced.with(Serdes.String(), topStationsResultSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            shutdownLatch.countDown();
        }));

        streams.start();
        System.out.println("Kafka Streams application started.");

        try {
            shutdownLatch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static String extractTripDate(String startTime) {
        int separatorIndex = startTime.indexOf(' ');
        if (separatorIndex > 0) {
            return startTime.substring(0, separatorIndex);
        }
        return startTime;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    public static class TripEvent {
        @JsonProperty("trip_id")
        private String tripId;

        @JsonProperty("start_time")
        private String startTime;

        @JsonProperty("trip_duration_seconds")
        private Double tripDurationSeconds;

        @JsonProperty("from_station")
        private Station fromStation;

        @JsonProperty("to_station")
        private Station toStation;

        public String getTripId() {
            return tripId;
        }

        public void setTripId(String tripId) {
            this.tripId = tripId;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public Double getTripDurationSeconds() {
            return tripDurationSeconds == null ? 0.0 : tripDurationSeconds;
        }

        public void setTripDurationSeconds(Double tripDurationSeconds) {
            this.tripDurationSeconds = tripDurationSeconds;
        }

        public Station getFromStation() {
            return fromStation;
        }

        public void setFromStation(Station fromStation) {
            this.fromStation = fromStation;
        }

        public Station getToStation() {
            return toStation;
        }

        public void setToStation(Station toStation) {
            this.toStation = toStation;
        }

        public String getFromStationName() {
            return fromStation == null ? null : fromStation.getName();
        }

        public String getToStationName() {
            return toStation == null ? null : toStation.getName();
        }
    }

    public static class Station {
        @JsonProperty("id")
        private String id;

        @JsonProperty("name")
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class AverageAccumulator {
        private long tripCount;
        private double totalDurationSeconds;

        public AverageAccumulator addTrip(double durationSeconds) {
            tripCount += 1;
            totalDurationSeconds += durationSeconds;
            return this;
        }

        public AverageDurationResult toResult(String day) {
            double average = tripCount == 0 ? 0.0 : totalDurationSeconds / tripCount;
            return new AverageDurationResult(day, tripCount, totalDurationSeconds, average);
        }

        public long getTripCount() {
            return tripCount;
        }

        public void setTripCount(long tripCount) {
            this.tripCount = tripCount;
        }

        public double getTotalDurationSeconds() {
            return totalDurationSeconds;
        }

        public void setTotalDurationSeconds(double totalDurationSeconds) {
            this.totalDurationSeconds = totalDurationSeconds;
        }
    }

    public static class AverageDurationResult {
        private String tripDate;
        private long tripCount;
        private double totalDurationSeconds;
        private double averageDurationSeconds;

        public AverageDurationResult() {
        }

        public AverageDurationResult(String tripDate, long tripCount, double totalDurationSeconds, double averageDurationSeconds) {
            this.tripDate = tripDate;
            this.tripCount = tripCount;
            this.totalDurationSeconds = totalDurationSeconds;
            this.averageDurationSeconds = averageDurationSeconds;
        }

        public String getTripDate() {
            return tripDate;
        }

        public void setTripDate(String tripDate) {
            this.tripDate = tripDate;
        }

        public long getTripCount() {
            return tripCount;
        }

        public void setTripCount(long tripCount) {
            this.tripCount = tripCount;
        }

        public double getTotalDurationSeconds() {
            return totalDurationSeconds;
        }

        public void setTotalDurationSeconds(double totalDurationSeconds) {
            this.totalDurationSeconds = totalDurationSeconds;
        }

        public double getAverageDurationSeconds() {
            return averageDurationSeconds;
        }

        public void setAverageDurationSeconds(double averageDurationSeconds) {
            this.averageDurationSeconds = averageDurationSeconds;
        }
    }

    public static class DailyTripCountResult {
        private String tripDate;
        private long tripCount;

        public DailyTripCountResult() {
        }

        public DailyTripCountResult(String tripDate, long tripCount) {
            this.tripDate = tripDate;
            this.tripCount = tripCount;
        }

        public String getTripDate() {
            return tripDate;
        }

        public void setTripDate(String tripDate) {
            this.tripDate = tripDate;
        }

        public long getTripCount() {
            return tripCount;
        }

        public void setTripCount(long tripCount) {
            this.tripCount = tripCount;
        }
    }

    public static class MostPopularStationAccumulator {
        private Map<String, Long> stationCounts = new HashMap<>();

        public MostPopularStationAccumulator addStation(String stationName) {
            if (stationName == null || stationName.isBlank()) {
                return this;
            }
            stationCounts.merge(stationName, 1L, Long::sum);
            return this;
        }

        public MostPopularStationResult toResult(String day) {
            String stationName = null;
            long count = 0;

            for (Map.Entry<String, Long> entry : stationCounts.entrySet()) {
                if (entry.getValue() > count || (entry.getValue() == count && compareNames(entry.getKey(), stationName) < 0)) {
                    stationName = entry.getKey();
                    count = entry.getValue();
                }
            }

            return new MostPopularStationResult(day, stationName, count);
        }

        public Map<String, Long> getStationCounts() {
            return stationCounts;
        }

        public void setStationCounts(Map<String, Long> stationCounts) {
            this.stationCounts = stationCounts == null ? new HashMap<>() : stationCounts;
        }
    }

    public static class MostPopularStationResult {
        private String tripDate;
        private String stationName;
        private long tripCount;

        public MostPopularStationResult() {
        }

        public MostPopularStationResult(String tripDate, String stationName, long tripCount) {
            this.tripDate = tripDate;
            this.stationName = stationName;
            this.tripCount = tripCount;
        }

        public String getTripDate() {
            return tripDate;
        }

        public void setTripDate(String tripDate) {
            this.tripDate = tripDate;
        }

        public String getStationName() {
            return stationName;
        }

        public void setStationName(String stationName) {
            this.stationName = stationName;
        }

        public long getTripCount() {
            return tripCount;
        }

        public void setTripCount(long tripCount) {
            this.tripCount = tripCount;
        }
    }

    public static class TopStationsAccumulator {
        private Map<String, Long> stationCounts = new HashMap<>();

        public TopStationsAccumulator addTrip(String fromStationName, String toStationName) {
            addStation(fromStationName);
            addStation(toStationName);
            return this;
        }

        public TopStationsAccumulator addStation(String stationName) {
            if (stationName == null || stationName.isBlank()) {
                return this;
            }
            stationCounts.merge(stationName, 1L, Long::sum);
            return this;
        }

        public TopStationsResult toResult(String day, int limit) {
            List<StationCount> leaders = stationCounts.entrySet().stream()
                    .filter(entry -> entry.getKey() != null)
                    .sorted(Comparator
                            .comparingLong((Map.Entry<String, Long> entry) -> entry.getValue()).reversed()
                            .thenComparing(Map.Entry::getKey))
                    .limit(limit)
                    .map(entry -> new StationCount(entry.getKey(), entry.getValue()))
                    .toList();

            return new TopStationsResult(day, new ArrayList<>(leaders));
        }

        public Map<String, Long> getStationCounts() {
            return stationCounts;
        }

        public void setStationCounts(Map<String, Long> stationCounts) {
            this.stationCounts = stationCounts == null ? new HashMap<>() : stationCounts;
        }
    }

    public static class TopStationsResult {
        private String tripDate;
        private List<StationCount> stations = new ArrayList<>();

        public TopStationsResult() {
        }

        public TopStationsResult(String tripDate, List<StationCount> stations) {
            this.tripDate = tripDate;
            this.stations = stations;
        }

        public String getTripDate() {
            return tripDate;
        }

        public void setTripDate(String tripDate) {
            this.tripDate = tripDate;
        }

        public List<StationCount> getStations() {
            return stations;
        }

        public void setStations(List<StationCount> stations) {
            this.stations = stations == null ? new ArrayList<>() : stations;
        }
    }

    public static class StationCount {
        private String stationName;
        private long usageCount;

        public StationCount() {
        }

        public StationCount(String stationName, long usageCount) {
            this.stationName = stationName;
            this.usageCount = usageCount;
        }

        public String getStationName() {
            return stationName;
        }

        public void setStationName(String stationName) {
            this.stationName = stationName;
        }

        public long getUsageCount() {
            return usageCount;
        }

        public void setUsageCount(long usageCount) {
            this.usageCount = usageCount;
        }
    }

    private static int compareNames(String left, String right) {
        if (Objects.equals(left, right)) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return left.compareTo(right);
    }
}
