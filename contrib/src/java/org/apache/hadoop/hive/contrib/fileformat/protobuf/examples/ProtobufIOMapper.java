
package protobuf.examples;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;

import protobuf.mapred.ProtobufInputFormat;


public class ProtobufIOMapper extends MapReduceBase 
    implements Mapper<LongWritable, BytesWritable, Text, BytesWritable> {

    public void map(LongWritable key, BytesWritable value,
            OutputCollector<Text, BytesWritable> output, Reporter reporter)
        throws IOException {
        output.collect(new Text("a"),value); 
    }
}

