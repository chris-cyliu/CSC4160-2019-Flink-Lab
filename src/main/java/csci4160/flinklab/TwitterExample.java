package csci4160.flinklab;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

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
        // emit result to the dashboard through websocket
        SinkFunction<Tuple2<String, Integer>> webSocketSink = new WebsocketSinkFunction<>();
        tweets.addSink(webSocketSink).name("WebSocketSink");

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

    public static class WebsocketSinkFunction<IN> extends RichSinkFunction<IN> {

        private static final long serialVersionUID = 1L;

        private transient static Logger log = LoggerFactory.getLogger(WebsocketSinkFunction.class);

        private transient ObjectMapper objectMapper;

        private transient MyWebSocketServer webSocketServer;

        private int port = 18082;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.log.info("Going to listern port:" + port);
            webSocketServer = new MyWebSocketServer(new InetSocketAddress("0.0.0.0", port));
            objectMapper = new ObjectMapper();
            webSocketServer.start();
        }

        @Override
        public void invoke(IN value, Context context) throws Exception {
            webSocketServer.emit(objectMapper.writeValueAsString(value));
        }

        @Override
        public String toString() {
            return  WebsocketSinkFunction.class.toString();
        }

        @Override
        public void close() throws Exception {
            super.close();
            this.webSocketServer.stop();
        }

        private static class MyWebSocketServer extends WebSocketServer {

            private Set<WebSocket> websockets;

            private Logger log = LoggerFactory.getLogger(WebSocketServer.class);

            public MyWebSocketServer(InetSocketAddress address) {
                super(address);
                websockets = new HashSet<>();
            }

            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                websockets.add(conn);
                this.log.info("new connection to " + conn.getRemoteSocketAddress());
            }

            @Override
            public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                websockets.remove(conn);
                this.log.info("closed " + conn.getRemoteSocketAddress() + " with exit code " + code + " additional info: " + reason);
            }

            @Override
            public void onMessage(WebSocket conn, String message) {
                this.log.info("received message from "	+ conn.getRemoteSocketAddress() + ": " + message);
            }

            @Override
            public void onMessage( WebSocket conn, ByteBuffer message ) {
                this.log.info("received ByteBuffer from "	+ conn.getRemoteSocketAddress());
            }

            @Override
            public void onError(WebSocket conn, Exception ex) {
                System.err.println("an error occured on connection " + conn.getRemoteSocketAddress()  + ":" + ex);
            }

            @Override
            public void onStart() {
                this.log.info("server started successfully");
            }

            public void emit(String string){
                for(WebSocket websocket: this.websockets){
                    try {
                        websocket.send(string);
                    }catch(Exception e){
                        this.log.error("Fail to send data. Skip it",e);
                    }
                }
            }

        }
    }
}