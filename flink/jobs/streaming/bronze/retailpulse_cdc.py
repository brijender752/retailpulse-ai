from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction


class PrintCDCEvent(MapFunction):

    def map(self, value):
        print("=" * 80)
        print("CDC EVENT")
        print(value)
        print("=" * 80)

        return value


def main():

    # ---------------------------------------------------------
    # 1. Create Flink execution environment
    # ---------------------------------------------------------

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    # ---------------------------------------------------------
    # 2. Kafka source
    # ---------------------------------------------------------

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("retailpulse.ecommerce.customers")
        .set_group_id("retailpulse-flink-cdc")
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    # ---------------------------------------------------------
    # 3. Create Kafka stream
    # ---------------------------------------------------------

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka CDC Source",
    )

    # ---------------------------------------------------------
    # 4. Process CDC events
    # ---------------------------------------------------------

    processed_stream = stream.map(
        PrintCDCEvent()
    )

    # ---------------------------------------------------------
    # 5. Execute
    # ---------------------------------------------------------

    processed_stream.print()

    env.execute(
        "RetailPulse PyFlink Kafka CDC"
    )


if __name__ == "__main__":
    main()