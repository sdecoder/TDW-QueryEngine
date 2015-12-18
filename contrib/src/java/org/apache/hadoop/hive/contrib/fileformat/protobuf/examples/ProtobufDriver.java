package protobuf.examples;


import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import protobuf.examples.ProtobufMapper;
import protobuf.mapred.ProtobufInputFormat;


public class ProtobufDriver extends Configured implements Tool {

  //@Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Protobuf Test");
    conf.setInputFormat(ProtobufInputFormat.class);
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(ProtobufMapper.class);
    //conf.setReducerClass(MaxTemperatureReducer.class);
    
    //DistributedCache.addCacheFile(new URI("/user/bryan/protobuf-java-2.3.0.jar"), conf);
    DistributedCache.addFileToClassPath(new Path("/user/bryan/","protobuf-java-2.3.0.jar"), conf);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ProtobufDriver(), args);
    System.exit(exitCode);
  }
}
