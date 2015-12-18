package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.ComparisonOpMethodResolver;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@description(
    name = "comparison",
    value= "a _FUNC_ b - Returns 1 if a > b , 0 if a = b, -1 if a < b."
)
public class UDFComparison extends UDF {

  IntWritable resultCache;
  public UDFComparison() {
    // super(null);
    // setResolver(new ComparisonOpMethodResolver(this.getClass()));
    
    resultCache = new IntWritable();
  }
  
  public IntWritable evaluate(Text a, Text b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }
  
  public IntWritable evaluate(ByteWritable a, ByteWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }
  
  public IntWritable evaluate(ShortWritable a, ShortWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }
  
  public IntWritable evaluate(IntWritable a, IntWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }
  
  public IntWritable evaluate(LongWritable a, LongWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }
  
  public IntWritable evaluate(FloatWritable a, FloatWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }
  
  public IntWritable evaluate(DoubleWritable a, DoubleWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }
  
}
