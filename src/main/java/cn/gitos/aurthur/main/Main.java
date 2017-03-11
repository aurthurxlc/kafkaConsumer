package cn.gitos.aurthur.main;

import cn.gitos.aurthur.avro.RcvblFlow;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Aurthur on 2017/3/11.
 */
public class Main {
    public final static String TOPIC = "TEST-TOPIC";

    public static void main(String[] args) {
        try {
            //消息生产
            consume();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void consume() throws IOException {
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.0.75:2181");
        props.put("group.id", "1");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC);
        KafkaStream stream = streams.get(0);

        ConsumerIterator<String, byte[]> it = stream.iterator();
        while (it.hasNext()) {
            try {
                DatumReader<RcvblFlow> reader = new SpecificDatumReader<RcvblFlow>(RcvblFlow.class);
                Decoder decoder = DecoderFactory.get().binaryDecoder(it.next().message(), null);
                RcvblFlow msg = reader.read(null, decoder);

                System.out.println(msg.getRcvblAmtId() + "," + msg.getConsNo() + "," + msg.getReleasedDate());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (consumer != null)
            consumer.shutdown();
    }

}
