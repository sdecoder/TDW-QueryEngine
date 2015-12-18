package protobuf.examples;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import protobuf.mapred.ProtobufRecordReader;

import org.hello.Bar.TestMsg;

public class ProtobufMapper extends MapReduceBase implements
		Mapper<LongWritable, BytesWritable, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(ProtobufMapper.class);

	public void map(LongWritable key, BytesWritable value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		LOG.info("In Mapper Get Data: " + value.toString());
		
		/*
		 * didn't work this way(InvalidProtocolBufferException
		 * Protocol message contained an invalid tag (zero))
		 * 
		 * TestMsg msg = TestMsg.parseFrom(value.getBytes());
		*/	
		int bufferSize = value.getLength();
		byte buffer[] = new byte[bufferSize];
		System.arraycopy(value.getBytes(),0,buffer,0,bufferSize);
		
		TestMsg msg = TestMsg.parseFrom(buffer);	
		
		output.collect(new Text(msg.getEmail()), new IntWritable(msg.getId()));
	}
}
