package csci4160.flinklab;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * <p>Usage: <code>Usage: TwitterExample [--output &lt;path&gt;]
 * [--twitter-source.consumerKey &lt;key&gt; --twitter-source.consumerSecret &lt;secret&gt; --twitter-source.token &lt;token&gt; --twitter-source.tokenSecret &lt;tokenSecret&gt;]</code><br>
 */
public class TwitterExample {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        // Define the Twitter Source
        DataStream<String> streamSource = env.addSource(new TwitterSource(params.getProperties()));

        // Transform
        DataStream<Tuple2<String, Integer>> tweets = streamSource
                // selecting English tweets and splitting to (word, 1)
                .flatMap(new GetHashTags())
                // group by words and sum their occurrences
                .keyBy(0)
                .sum(1);

        // emit result to the standard output
        SinkFunction<Tuple2<String, Integer>> printSink = new PrintSinkFunction<>();
        tweets.addSink(printSink).name("StdoutSink");

        // execute program
        env.execute("Twitter Streaming Example");
    }

    // *************************************************************************
    // Custom FlatMap Function
    // *************************************************************************
    public static class GetHashTags implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            // deserialize the tweet json to JsonNode object
            JsonNode jsonNode = jsonParser.readTree(value);

            //get the hashtags
            try {
                JsonNode jsonNodeHashTags = jsonNode.get("entities").get("hashtags");
                for (JsonNode jsonNodeHashTag: jsonNodeHashTags){
                    String hashtag = jsonNodeHashTag.get("text").textValue();
                    out.collect(new Tuple2<>(hashtag, 1));
                }
            }catch(Exception e) {
                //System.out.println("No hashtag in tweets. Skip it ");
            }
        }
    }
}