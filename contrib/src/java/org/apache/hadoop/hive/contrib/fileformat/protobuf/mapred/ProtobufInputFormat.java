/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package protobuf.mapred;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;




/** An {@link InputFormat} for protocol buffer file.  Files are broken into records.
 * A record is consisted by a varint32 length + buffer.  Keys are
 * the position in the file, and values are the buffer of protobuf record 
 * @deprecated Use {@link protobuf.mapreduce.ProtobufInputFormat}
 *  instead.
 */
//@Deprecated
public class ProtobufInputFormat extends FileInputFormat<LongWritable, BytesWritable>
  implements JobConfigurable {

   
  public void configure(JobConf conf) {
    
  }
  
  protected boolean isSplitable(FileSystem fs, Path file) {
	return false; // need a index file to make the protobuf file splitable

  }

  public RecordReader<LongWritable, BytesWritable> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
    throws IOException {
    
    reporter.setStatus(genericSplit.toString());
    return new ProtobufRecordReader(job, (FileSplit) genericSplit);
  }
}

