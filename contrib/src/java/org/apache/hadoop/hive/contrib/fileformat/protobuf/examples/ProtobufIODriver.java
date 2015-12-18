package protobuf.examples;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import protobuf.examples.ProtobufIOMapper;
import protobuf.mapred.ProtobufInputFormat;
import protobuf.mapred.ProtobufOutputFormat;


public class ProtobufIODriver extends Configured implements Tool {


  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Protobuf Test");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.setInputFormat(ProtobufInputFormat.class);
    conf.setOutputFormat(ProtobufOutputFormat.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(BytesWritable.class);

    conf.setMapperClass(ProtobufIOMapper.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ProtobufIODriver(), args);
    System.exit(exitCode);
  }
}
