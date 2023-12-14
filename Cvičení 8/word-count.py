import logging
import sys

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import (FileSource, StreamFormat)

def word_count(input_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    # Nastavneí dávkového zpracování
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    source = FileSource \
    .for_record_stream_format(StreamFormat.text_line_format(), input_path) \
    .process_static_file_set() \
    .build()

    # define the source
    ds = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )

    def split(line):
        yield from [str.join("", filter(str.isalpha, x))[0] for x in line.lower().split()]

    # compute word count
    ds = ds\
        .flat_map(split) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    word_count("/files/data/")