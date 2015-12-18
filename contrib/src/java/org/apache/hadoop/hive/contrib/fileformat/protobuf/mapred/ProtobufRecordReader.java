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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;


/**
 *  Treats keys as offset in file and value as protobuf . 
 */

public class ProtobufRecordReader implements RecordReader<LongWritable, BytesWritable> {

    private static final Log LOG = LogFactory.getLog(ProtobufRecordReader.class);

    private static final int DEFAULT_BUFFER_SIZE = 8*1024*1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private byte pool[];
    private int bufferPos;	
    private int bufferLen;
    private static final int MAX_VARINT32_LEN = 5;

    private long start;
    private long pos;
    private long end;
    private FSDataInputStream in;
    protected Configuration conf;

    public ProtobufRecordReader(Configuration conf, FileSplit split) throws IOException {

	//FileSplit split = (FileSplit) genericSplit;
	this.conf = conf;

	start = split.getStart();
	end = start + split.getLength();
	final Path file = split.getPath();

	// open the file and seek to the start of the split
	FileSystem fs = file.getFileSystem(conf);
	in = fs.open(split.getPath());

	if (start != 0) 
	{
	    in.seek(start);
	}
	this.pos = start;

	pool = new byte[this.bufferSize];
	bufferPos = this.bufferSize-1;
	bufferLen = this.bufferSize;

    }

    public LongWritable createKey() {
	return new LongWritable();
    }

    public BytesWritable createValue() {
	return new BytesWritable();
    }

    /** 
     * Returns the current position in the input.
     * 
     * @return the current position in the input.
     * @throws IOException
     */    
    public long getPos() throws IOException{
	return pos;
    }

    public boolean poolRefill(long beginPos)
    {
	if(beginPos + bufferLen > end )
	{
	    bufferLen = (int)(end - beginPos);
	    LOG.info("in do refill bufferLen :" + bufferLen);
	}
	    
	try
	{
	    int ret = in.read(beginPos,pool,0,bufferLen);
	    if(ret == -1)
	    {
		LOG.info("in do refill Error");
		return false;
	    }
	    else 
	    {
		//LOG.info("in do refill OK");
		bufferPos = 0;
		bufferLen = ret;   
		return true;
	    }
	}
	catch(IOException e)
	{
	    LOG.info("in do refill Error IOException");
	    return false;
	}

    }

    /** Read key/value pair in a line. */
    public synchronized boolean next(LongWritable key, BytesWritable value) throws IOException {
	//LOG.info("New record at pos: " + pos);
	//LOG.info("bufferLen,bufferPos"+bufferLen +" " +bufferPos);

	if (bufferLen - bufferPos < MAX_VARINT32_LEN)
	{
	    //LOG.info("do refill");
	    boolean bingo = poolRefill(pos) ;
	    if(!bingo )
	    {
		LOG.info("do refill Error");
		key = null;
		value = null;
		return false;
	    }    		
	}

	//LOG.debug("New record at pos: " + pos);
	
	key.set(pos);
	int firsByte = (int)pool[bufferPos];
	bufferPos++;
	pos++;
	int size = readRawVarint32(firsByte);
	//LOG.info("New record size: " + size +"[" +(pos-1)+"]");

	if(bufferLen - bufferPos < size)
	{
	    //LOG.info("do refill");
	    boolean bingo = poolRefill(pos);
	    if(!bingo)
	    {
		LOG.info("do refill Error");
		key = null;
		value = null;
		return false;
	    }    	

	}
	//LOG.debug("Read a record of size: " + size );
	//LOG.debug("New record buffer begin at pos: " + pos);
	// byte newData[] = new byte[size];
	//int nbytes = in.read(pos,newData,0,size);

	value.set(pool,bufferPos,size);
	bufferPos += size;        
	pos += size;      
	//LOG.info("Get newData: " + value.toString());
	return true;
    }



    /**
     * Get the progress within the split
     */

    //@Override
    public float getProgress() {
	if (start == end) {
	    return 0.0f;
	} else {
	    return Math.min(1.0f, (pos - start) / (float) (end - start));
	}
    }

    public synchronized void close() throws IOException {
	if (in != null) {
	    in.close();
	}
    }

    public int readRawVarint32(int firstByte)
    {
	if ((firstByte & 0x80) == 0) {
	    return firstByte;
	}

	int result = firstByte & 0x7f;
	int offset = 7;
	for (; offset < 32; offset += 7) 
	{
	    byte b = pool[bufferPos];
	    bufferPos++;
	    pos++;
	    result |= (b & 0x7f) << offset;
	    if ((b & 0x80) == 0) {
		return result;
	    }
	}
	// Keep reading up to 64 bits.
	for (; offset < 64; offset += 7) {
	    byte b = pool[bufferPos];
	    bufferPos++;
	    pos++;
	    if ((b & 0x80) == 0) {
		return result;
	    }
	}
	return result;
	//throw new IOException("PB format error"); 
    }


}

