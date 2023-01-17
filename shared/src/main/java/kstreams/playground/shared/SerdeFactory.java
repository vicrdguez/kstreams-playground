package kstreams.playground.shared;

import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {

  public static <T extends SpecificRecord> SpecificAvroSerde<T> getAvroSerde() {
    SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();

    serde.configure(Maps.newHashMap(Maps.fromProperties(AppContext.getConfig())),
        false);

    return serde;
  }

  public static <T> Serde<T> getJsonSerde(final Class<T> clazz, final boolean isKey) {
    final String typeConfigProperty = isKey
        ? KafkaJsonDeserializerConfig.JSON_KEY_TYPE
        : KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;
    Map<String, Class<T>> props;

    if (clazz == null){
      props = Collections.emptyMap();
    } else {
      props = Collections.singletonMap(typeConfigProperty, clazz);
    }

    KafkaJsonSerializer<T> serializer = new KafkaJsonSerializer<T>();
    serializer.configure(props, isKey);
    KafkaJsonDeserializer<T> deserializer = new KafkaJsonDeserializer<T>();
    deserializer.configure(props, isKey);

    return Serdes.serdeFrom(serializer, deserializer);
  }

}
