import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class WordCounter {
    private static final String INPUT_WORD_COUNT_TOPIC = "word-count-input";
    private static final String OUTPUT_WORD_COUNT_TOPIC = "word-count-output";

    public static void main(String[] args) {
        Properties props = KafkaUtils.setProperties("streams-word-counter");
        final StreamsBuilder builder = new StreamsBuilder();
        countWords(builder);
        KafkaUtils.run(builder, props);
    }

    private static void countWords(StreamsBuilder builder) {
        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // Construct a `KStream` from the input topic "word-count-input", where message values represent lines of text
        KStream<String, String> textLines = builder.stream(INPUT_WORD_COUNT_TOPIC, Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.
                .flatMapValues(value -> {
                    log.info("map: {}", value);
                    return Arrays.asList(value.toLowerCase().split("\\W+"));
                })

                // Group the text words as message keys
                .groupBy((key, value) -> {
                    log.info("group: key: {}, value: {}", key, value);
                    return value;
                })

                // Count the occurrences of each word (message key).
                .count();

        // Store the running counts as a changelog stream to the output topic.
        wordCounts.toStream().to(OUTPUT_WORD_COUNT_TOPIC, Produced.with(stringSerde, longSerde));
    }
}
