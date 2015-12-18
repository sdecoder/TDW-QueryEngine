package org.apache.hadoop.hive.ql.exec;

//~--- non-JDK imports --------------------------------------------------------

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.tools.sysinfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.forwardDesc;
import org.apache.hadoop.hive.ql.plan.sampleDesc;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector.MyField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.omg.PortableInterceptor.ForwardRequest;
import org.omg.PortableServer._ServantActivatorStub;
import org.apache.hadoop.hive.ql.plan.sampleDesc;
import org.apache.hadoop.io.Text;

//~--- JDK imports ------------------------------------------------------------

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.io.Serializable;
import java.math.*;

// sampler[processor] => histogram[cache]
public class SampleOperator extends Operator<sampleDesc> implements
	Serializable {
    /** 进行采样统计时采样的比例 **/
    public static final byte streamTag = (byte) 0xf1;

    public static final String STATISTICS_SAMPLING_FACTOR_ATTR = "STATISTICS_SAMPLING_FACTOR";
    public static final String STATISTICS_SAMPLING_WINDOW_ATTR = "Sampling.Window";
    public static final String JOBSPLITSIZE_ATTR = "CONFIGURATION_JOBSPLITTER_SIZE";
    public static final String SAMPLE_COUNTER_ATTR = "SAMPLE_COUNTER";
    public static final String SAMPLE_DATA_ATTR = "SAMPLE_DATA";    

    public enum workingMode {
	ETL_Row, ETL_Unit, Analyze_Row, Analyze_Unit
    };

    String _finalTableName_;
    StringBuilder _hStringBuilder;
    int numrows = 0;
    int rowstoskip = -1;

    int cur_samplerows; // this is index
    int samplerows = 0;

    int cur_sample_records;
    int num_sample_records;
    int num_sample_splits;
    int _sampleWindow_;
    
    int objectCount;
    int sumLenOfPreTenObjs;
    int estimatedSplitSize;
    float samplingFactor;
    workingMode currentWorkingMode;
    boolean firstEstimate;

    Object rowSet[];
    Object unitSet[];
    SamplerData sd;
    String _outputKey;
    String _tableName_;   

    
    ArrayList<Integer> replaceTracker;
    @Deprecated
    int sortedID[];

    public SampleOperator() {
	numrows = 0;
	firstEstimate = true;
	this._outputKey = null;
    }

    public void setOutputKey(String para) {
	this._outputKey = para;
    }

    public String getOutputKey() {
	return this._outputKey;
    }

    long inputSplitFileLength;
    long inputSplitFileStart;

    protected void initializeOp(Configuration hconf) throws HiveException {
	// you must decide this split should be sampled or not;
	//System.out.println("[Trace]SampleOperator.initializeOp: Initialized");
	super.initializeOp(hconf);
	_hStringBuilder = new StringBuilder();
	_tableName_ = conf.getTableName();
	
	if (_tableName_ == null) {
	    _tableName_ = "";
	}
	
	String _path = hconf.get("map.input.file");
	inputSplitFileStart = hconf.getLong("map.input.start", -1);
	inputSplitFileLength = hconf.getLong("map.input.length", -1);

	System.out.println("map.input.file is " + _path);
	if (inputSplitFileStart == -1) {
	    System.out.println("map.input.start not set...");
	} else {
	    System.out.println("map.input.start is " + inputSplitFileStart);
	}
	if (inputSplitFileLength == -1) {
	    System.out.println("map.input.length not set...");
	} else {
	    System.out.println("map.input.length is " + inputSplitFileLength);
	}
	
	num_sample_records = 0 ;
	objectCount = 0;
	_sampleWindow_ = conf.getNumSampleRecords();
	
	//System.out.println("[Trace]SampleOperator.initializeOp: Terminated");
    }

    @Deprecated
    public void sortById() throws Exception {
	sortedID = new int[num_sample_records + 1];
	for (int i = 1; i <= num_sample_records; i++) {
	    sortedID[i] = i;
	}
	Iterator<Integer> axiter = replaceTracker.iterator();
	while (axiter.hasNext()) {
	    Integer axi = axiter.next();
	    for (int i = 1; i <= num_sample_records; i++) {
		if (axi < sortedID[i]) {
		    sortedID[i]--;
		}
	    }
	    sortedID[axi] = num_sample_records;
	}

    }

    int rowLength = 0;
    boolean _firstEstimate = true;
    long _estimateRowNumbers = 0;    
    long step = 1;
    
    long timeReg = 0;
    long timeSec = 0;
    
    
    public void process(Object row, int tag) throws HiveException {
	// System.out.println("[Trace]SampleOperator.process");
	// System.out.println("\t[Trace] The tag is " + tag);
	objectCount++;	
	
	if (objectCount <= 10) {
	    forward(row, null); // to Histogram operator
	    num_sample_records++;
	    Object [] _oArray = (Object []) row;
	    //List<Object> _fields = _ls.getFieldsAsList();
	    for (Object _obj : _oArray) {
		rowLength += _obj.toString().length();
	    }

	} else {
	    if (_firstEstimate) {
		_firstEstimate = false;
		rowLength = rowLength / objectCount;		
		_estimateRowNumbers = inputSplitFileLength / rowLength;		
		step = (long)( _estimateRowNumbers / _sampleWindow_) ;
		if (step < 1 ) {
		    step = 1;
		}
		System.out.println("[SampleOp] _estimateRowNumbers: " + _estimateRowNumbers  + " step:" + step );
	    }
	    if (objectCount % step == 0) {
		forward(row, null);
		num_sample_records++;
	    } 
	    
	}	
	
	// try {
	// while (true) {
	// if (cur_samplerows < numrows) { // whole set index;
	// if (cur_sample_records < num_sample_records) { // sample window index
	// int K = numrows - cur_samplerows;
	// int k = num_sample_records - cur_samplerows;
	// if (k >= K) {
	// if (numrows < num_sample_records) {
	// rowSet[numrows++] = row;
	// rowSet[cur_sample_records ++] = row;
	// forward(row, null);
	// } else if (numrows == num_sample_records) {
	// if (rowstoskip < 0) {
	// Double rstate = init_selection_state(num_sample_records);
	// rowstoskip = (int) get_next_S(samplerows,
	// num_sample_records, rstate);
	// if (rowstoskip < 0) {
	// int idx = (int) (num_sample_records * (new Random()).nextDouble());
	// rowSet[idx] = row;
	// rowstoskip--;
	// }
	//	
	// }
	// }
	//								
	// } else {
	// Random rand = new Random();
	// int V = (rand.nextInt() + 1) / (0x7FFFFFFF + 2);
	// double p = 1 - (double) k / (double) K;
	// while (true) {
	// if (V < p) {
	// sd.cur_unit++;
	// K--;
	// p = p * (1 - (double) k / (double) K);
	// } else
	// break;
	//	
	// }
	// }
	//	
	// } else
	// break;
	// } else
	// break;
	// }
	// if (numrows < num_sample_records) {
	// } else {
	// // TODO: 如果采样得到记录数等于采样数组的大小，说明采样数组可能发生了替换现象，
	// // 这是对采样数组内的记录进行按照记录号排序。
	// // // sortById(); - TDW_STATISTICS_TYPE_CORRELATION
	// }
	//	
	// } catch (Exception generalExp) {
	// generalExp.printStackTrace();
	// }
	//
	// }
	// }

    }

    @Deprecated
    public void processUnit(Unit unitPara, int tag) throws HiveException {
	// process logic for unit

	assert (false);
	try {
	    while (true) {
		if (sd.cur_unit < sd.num_units) {
		    if (sd.cur_sampled_units < sd.num_samples) {

			// remained units number;
			int K = sd.num_units - sd.cur_unit;

			// needed sampled units number;
			int k = sd.num_samples - sd.cur_sampled_units;

			if (k >= K) {
			    // SWITCH: this logic is only for Unit sample;
			    // sample records in this sd.cur_unit;
			    if (numrows < num_sample_records) {
				unitSet[numrows] = getUnitByUnitNumber(sd.cur_unit).getCurrentRow();
			    } else if (numrows == num_sample_records) {
				// we need replacement here;
				if (rowstoskip < 0) {
				    // a)
				    // 如果rowstoskip小于0，重新计算rowstoskip值：（计算函数如附录1所示。）
				    Double rstate = init_selection_state(num_sample_records);
				    rowstoskip = (int) get_next_S(samplerows, num_sample_records, rstate);
				    if (rowstoskip < 0) {
					// b)
					// 计算完的rowstoskip仍小于等于0，则替换以当前记录替换采样数据中的某条记录。
					// （替换的记录idx = num_sample_records *
					// random()）
					int idx = (int) (num_sample_records * (new Random()).nextDouble());
					unitSet[idx] = getUnitByUnitNumber(sd.cur_unit).getCurrentRow();
					// c) rowstoskip自减。
					rowstoskip--;
				    }

				}
			    }
			    samplerows++;

			    sd.cur_unit++;
			    sd.cur_sampled_units++;
			} else {

			    // k < K :
			    // pick one unit from the remaining unit;
			    Random rand = new Random();
			    int V = (rand.nextInt() + 1) / (0x7FFFFFFF + 2);
			    double p = 1 - (double) k / (double) K;

			    while (true) {
				if (V < p) {
				    // skip current unit;
				    sd.cur_unit++;
				    K--;
				    p = p * (1 - (double) k / (double) K);
				} else {
				    break;
				}
			    }
			}
		    } else
			break;
		} else
		    break;

	    }
	    if (numrows < num_sample_records) {

	    } else {
		// FIXME: TDW_STATISTICS_TYPE_CORRELATION
		// sortById();
	    }

	} catch (Exception generalException) {
	    generalException.printStackTrace();
	}
    }


    public void closeOp(boolean abort) throws HiveException {
	//System.out.println("[Trace] SampleOperator.closeOp: Initialized...");
	try {
	    _hStringBuilder.delete(0, _hStringBuilder.length());
	    _hStringBuilder.append(SAMPLE_COUNTER_ATTR);
	    _hStringBuilder.append(ToolBox.hiveDelimiter);
	    _hStringBuilder.append(_tableName_);	    
	    HiveKey _outputHiveKey = ToolBox.getHiveKey(
		     _hStringBuilder.toString() ,this.streamTag);
	    out.collect(_outputHiveKey, new Text(String.valueOf(num_sample_records)));
	} catch (Exception e) {
	    e.printStackTrace();
	}
	//System.out.println("[Trace] SampleOperator.closeOp: Terminated...");
	
	// try {
	//
	// String _tblName = conf.getTableName();
	// if (_tblName == null || _tblName.equals("")) {
	// System.out.println(
	// "[FATAL ERROR]: [SampleOperator.closeOp] No table name found");
	// return;
	// }
	//						
	
	// out.collect(_outputHiveKey, new
	// Text(String.valueOf(num_sample_records)));
	//			
	// for (int i = 0; i < num_sample_records; i++) {
	// HiveKey _key = getHiveKey(
	// SAMPLE_DATA_ATTR + ToolBox.dotDelimiter + _tblName);
	// 
	// }
	//
	// } catch (Exception e) {
	// e.printStackTrace();
	// }

    }

    public String getName() {
	return new String("SAMPLE");
    }

    @Deprecated
    protected Unit getUnitByUnitNumber(int unitNo) {
	// this is a dummy method and should be replaced in near future;
	return new Unit();
    }

    @Deprecated
    protected class SamplerData {
	int num_samples; // 需要采样的unit/row个数
	int cur_sampled_units; // 当前已经采样过的unit的个数
	int cur_unit; // 当前采样的压缩unit
	int num_units; // 该split中包含的压缩unit的个数

	int cur_sampled_rows;
	int cur_row;
	int num_rows;

	public SamplerData() {
	    num_samples = 0;
	    cur_sampled_units = 0; // 当前已经采样过的unit的个数
	    cur_unit = 0; // 当前采样的压缩unit
	    num_units = 0; // 该split中包含的压缩unit的个数

	    cur_sampled_rows = 0;
	    cur_row = 0;
	    num_rows = 0;

	}
    }

    @Deprecated
    protected class Unit {
	// this class is only a dummy type;
	int size;
	int index;
	Object row[];

	public Unit() {
	    size = 0;
	    index = 0;
	}

	Object getCurrentRow() {
	    // return row[index];
	    return null;
	}

    }

    static double init_selection_state(int n) {
	double val = (new Random()).nextDouble();
	return Math.exp(-Math.log(val) / n);
    }

    static double get_next_S(double t, int n, Double stateptr) {
	double S;

	/* The magic constant here is T from Vitter's paper */
	if (t <= (22.0 * n)) {
	    /* Process records using Algorithm X until t is large enough */
	    double V, quot;

	    V = (new Random()).nextDouble(); /* Generate V */
	    S = 0;
	    t += 1;
	    /* Note: "num" in Vitter's code is always equal to t - n */
	    quot = (t - (double) n) / t;
	    /* Find min S satisfying (4.1) */
	    while (quot > V) {
		S += 1;
		t += 1;
		quot *= (t - (double) n) / t;
	    }
	} else {
	    /* Now apply Algorithm Z */
	    double W = stateptr;
	    double term = t - (double) n + 1;

	    for (;;) {
		double numer, numer_lim, denom;
		double U, X, lhs, rhs, y, tmp;

		/* Generate U and X */
		U = (new Random()).nextDouble();
		X = t * (W - 1.0);
		S = Math.floor(X); /* S is tentatively set to floor(X) */
		/* Test if U <= h(S)/cg(X) in the manner of (6.3) */
		tmp = (t + 1) / term;
		lhs = Math.exp(Math.log(((U * tmp * tmp) * (term + S))
			/ (t + X))
			/ n);
		rhs = (((t + X) / (term + S)) * term) / t;
		if (lhs <= rhs) {
		    W = rhs / lhs;
		    break;
		}
		/* Test if U <= f(S)/cg(X) */
		y = (((U * (t + 1)) / term) * (t + S + 1)) / (t + X);
		if ((double) n < S) {
		    denom = t;
		    numer_lim = term + S;
		} else {
		    denom = t - (double) n + S;
		    numer_lim = t + 1;
		}
		for (numer = t + S; numer >= numer_lim; numer -= 1) {
		    y *= numer / denom;
		    denom -= 1;
		}
		W = Math.exp(-Math.log((new Random()).nextDouble()) / n); /*
									   * Generate
									   * W
									   * in
									   * advance
									   */
		if (Math.exp(Math.log(y) / n) <= (t + X) / t)
		    break;
	    }
	    stateptr = W;
	}
	return S;
    }

}
