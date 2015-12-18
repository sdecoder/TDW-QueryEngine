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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;

/** An {@link OutputFormat} that writes protobuf files. 
 * @deprecated Use 
 *   {@link protobuf.mapreduce.ProtobufOutputFormat} instead.
 */
@Deprecated
public class ProtobufOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class ProtobufRecordWriter<K, V> implements
			RecordWriter<K, V> {
		//private static final Log LOG = LogFactory.getLog(ProtobufRecordWriter.class);

		protected DataOutputStream out;

		public ProtobufRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		public static int computeRawVarint32Size(final int value) {
			if ((value & (0xffffffff << 7)) == 0)
				return 1;
			if ((value & (0xffffffff << 14)) == 0)
				return 2;
			if ((value & (0xffffffff << 21)) == 0)
				return 3;
			if ((value & (0xffffffff << 28)) == 0)
				return 4;
			return 5;
		}

		/**
		 * Write the object to the byte stream.
		 * @param o the object to print
		 * @throws IOException if the write throws, we pass it on
		 */
		private void writeProtobufObject(BytesWritable value)
				throws IOException {
			int serialized = value.getLength();
			int bufferSize = computeRawVarint32Size(serialized);
			
			//LOG.debug("Get newData len: " + serialized);
			//LOG.debug("NewData info: " + value.toString());
			byte buffer[] = new byte[bufferSize];
			int position = 0;
			boolean more = true;
			while (more) {
				if ((serialized & ~0x7F) == 0) {
					buffer[position++] = (byte) serialized;
					more = false;
				} else {
					buffer[position++] = (byte) ((serialized & 0x7F) | 0x80);
					serialized >>>= 7;
				}

			}
			out.write(buffer, 0, bufferSize);
			out.write(value.getBytes(), 0, value.getLength());

		}

		public synchronized void write(K key, V value) throws IOException {
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullValue) {
				return;
			} else {
				if (value instanceof BytesWritable) {
					writeProtobufObject((BytesWritable) value);
				}
			}
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}

	}

	public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {

		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new ProtobufRecordWriter<K, V>(fileOut);
	}
}
