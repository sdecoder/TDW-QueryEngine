package org.apache.hadoop.hive.ql.exec;

//~--- non-JDK imports --------------------------------------------------------

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.tools.sysinfo;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.get_database_args;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SampleOperator;
import org.apache.hadoop.hive.ql.exec.StatsCollectionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities.streamStatus;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.hadoop.hive.ql.plan.forwardDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.histogramDesc;
import org.apache.hadoop.hive.ql.plan.sampleDesc;
import org.apache.hadoop.hive.ql.plan.statsDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.omg.CORBA._PolicyStub;

import com.facebook.fb303.FacebookService.getCounter_args;

//~--- JDK imports ------------------------------------------------------------

//import hadoop sdk;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.util.*;

import javax.crypto.spec.IvParameterSpec;

// delimiter which are not allowed to appear in the sql statement:
// . , : ;

public class ComputationBalancerReducer {

    final String NUM_MCV_ATTR = "NumberOfMcv";
    final String NUM_BIN_ATTR = "NumberOfBin";
    public static final String CBR_TABLENAME_ATTR = "_TableName_";
    public static final String CBR_TABLEDATALOCATION_ATTR = "_TableDataLocation_";
    public static final String CBR_FLUSHFILEURI_ATTR = "_flushFile_uri_";

    // Table level info collection;
    int stat_num_records;
    int stat_num_units;
    int stat_sampled_counter;
    int _sampledRecordNumber_;

    // File system section;
    // Using DFS to get information of this table;
    long stat_total_size;
    long stat_num_files;
    long stat_num_blocks;

    // Field level info collection;
    int stat_null_counter;
    int stat_nullfac;
    int stat_avg_field_width;
    int stat_distinct_values;

    StringBuilder _hStringBuilder;
    String fldName;
    String funcName;
    String destURIString;

    String _finalTblName_;
    String _tableDataLocation_;

    int num_of_bins;
    FileSystem _fs;

    JobConf jc;
    Table table;
    Log LOG = LogFactory.getLog(this.getClass().getName());

    TreeMap<String, Integer> infoDict;
    TreeMap<String, String> epDict;
    TreeMap<String, TreeMap<String, Integer>> mcvList;
    HashMap<String, String> tblStructHashMap;

    ArrayList<Object> _testList;

    public ComputationBalancerReducer() {
	// System.out.println("[Trace] ComputationBalancerReducer() [construction method]");
	stat_num_records = 0;
	stat_num_units = 0;
	stat_sampled_counter = 0;

	stat_total_size = 0;
	stat_num_files = 0;
	stat_num_blocks = 0;

	stat_null_counter = 0;
	stat_nullfac = 0;
	stat_avg_field_width = 0;
	stat_distinct_values = 0;

	_sampledRecordNumber_ = 0;

	fldName = "";
	funcName = "";
	destURIString = "";
	_tableDataLocation_ = "";

	infoDict = new TreeMap<String, Integer>();
	mcvList = new TreeMap<String, TreeMap<String, Integer>>();
	epDict = new TreeMap<String, String>();
	tblStructHashMap = new HashMap<String, String>();
	_hStringBuilder = new StringBuilder();

    }

    public void setTableName(String cp) {
	_finalTblName_ = cp;
    }

    public String getTableName() {
	return _finalTblName_;
    }

    public void setTableDataLocation(String _copy) {
	this._tableDataLocation_ = _copy;
    }

    public String getTableDataLocation() {
	return this._tableDataLocation_;
    }

    public void setJobConf(JobConf para) {
	// System.out.println("[Trace]:ComputationBalancerReducer.setJobConf" );
	this.jc = para;
    }

    public void setDestURI(String para) {
	this.destURIString = para;
    }

    public String getDestURI() {
	return this.destURIString;
    }

    public void setNumberOfBins(int para) {
	this.num_of_bins = para;
    }

    public void reduce(BytesWritable wrappedKey, Text wrappedValue) {
	final String _key = ToolBox.getOriginalKey(wrappedKey);

	if (_key.startsWith(StatsCollectionOperator.FIELDLENGTH_ATTR)) {
	    Integer iValue = Integer.valueOf(wrappedValue.toString());
	    Integer _reg_ = infoDict.get(_key);
	    if (_reg_ == null) {
		_reg_ = iValue;
	    } else {
		_reg_ += iValue;
	    }
	    infoDict.put(_key, _reg_);

	} else if (_key.startsWith(StatsCollectionOperator.NULLCOUNTER_ATTR)) {
	    Integer iValue = Integer.valueOf(wrappedValue.toString());
	    Integer _reg_ = infoDict.get(_key);
	    if (_reg_ == null) {
		_reg_ = iValue;
	    } else {
		_reg_ += iValue;
	    }
	    infoDict.put(_key, _reg_);

	} else if (_key.startsWith(StatsCollectionOperator.RECORDSNUM_ATTR)) {
	    Integer iValue = Integer.valueOf(wrappedValue.toString());
	    stat_num_records += iValue;

	} else if (_key.startsWith(SampleOperator.SAMPLE_COUNTER_ATTR)) {
	    Integer iValue = Integer.valueOf(wrappedValue.toString());
	    _sampledRecordNumber_ += iValue;

	} else if (_key.startsWith(SampleOperator.SAMPLE_DATA_ATTR)) {
	    _testList.add(wrappedValue);

	} else if (_key.startsWith(HistogramOperator.MCVLIST_ATTR)) {
	    if (mcvList == null) {
		//System.out.println("\t[Trace] HistogramOperator.MCVLIST_ATTR: mcvList == null");
		return;
	    }
	    {
		StringTokenizer _hst_ = new StringTokenizer(wrappedValue.toString(), ToolBox.hiveDelimiter);
		String _true_value_ = _hst_.nextToken();
		String _true_fre_ = _hst_.nextToken();		
		TreeMap<String, Integer> _valfre_map_ = mcvList.get(_key);

		if (_valfre_map_ == null) {
		    _valfre_map_ = new TreeMap<String, Integer>();
		    _valfre_map_.put(_true_value_, Integer.valueOf(_true_fre_));
		    mcvList.put(_key, _valfre_map_);
		} else {
		    Integer _o_fre_ = _valfre_map_.get(_true_value_);
		    if (_o_fre_ == null) {
			_o_fre_ = Integer.valueOf(0);
		    }
		    _o_fre_ += Integer.valueOf(_true_fre_);
		    _valfre_map_.put(_true_value_, _o_fre_);

		}

		if (_valfre_map_.keySet().size() > 512) {
		    // System.out.println("In the cbr.reduce, MCVlist compact is trigged. The MCVList size is "
		    // +
		    // mcvList.keySet().size());
		    ToolBox _tb = new ToolBox();
		    // _tb.compactByDescendSort(_valfre_map_, 0.8);
		    _tb.compact(_valfre_map_, ToolBox.SortMethod.DescendSort, Integer.valueOf(512));
		}

	    }

	} else if (_key.startsWith(SampleOperator.STATISTICS_SAMPLING_FACTOR_ATTR)) {
	    Integer ax = Integer.valueOf(wrappedKey.toString());
	    stat_sampled_counter += ax;
	}

    }   

    public void close() {

	//System.out.println("[Trace] ComputationBalancerReducer.close.OutOfClose()");
	try {
	    //System.out.println("[Debug] ComputationBalancerReducer.closeOp.FlushInfoOut");
	    //System.out.println("[Debug] destURIString = " + destURIString);
	    if (stat_num_records == 0) {
		System.out.println("[ERROR] stat_num_records == 0, Abort.");
		return;
	    }
	    System.out.println("The sample record counter is " + _sampledRecordNumber_);
	    Path outPath = new Path(destURIString + java.util.UUID.randomUUID().toString());
	    _fs = outPath.getFileSystem(jc);	    
	    FSDataOutputStream _outputStream = _fs.create(outPath);
	    _outputStream.writeBytes(this.getTableName() + "\n");

	    flushTableStatsInfo(_outputStream);
	    flushDistinctValue(_outputStream);
	    flushMCVlist(_outputStream);
	    flushHistogram(_outputStream);
	    flushInfo_DFS(_outputStream);

	    _outputStream.close();

	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    void flushMCVlist(FSDataOutputStream out) throws Exception {
	
	out.writeBytes("MCVList\n");
	for (String _iter_outside_ : mcvList.keySet()) {
	    TreeMap<String, Integer> _TreeMap = mcvList.get(_iter_outside_);	    
	    out.writeBytes(_iter_outside_ + "\n");
	    ToolBox _tb = new ToolBox();
	    
	    for (String _s_ : _TreeMap.keySet()) {
		_tb.push(_s_, _TreeMap.get(_s_));
	    }
	    
	    if (_TreeMap.keySet().size() > 256) {
		
		_tb.compact(_TreeMap, ToolBox.SortMethod.DescendSort, Integer.valueOf(256));
	    } else {
		_tb.descendSort();
	    }

	    for (int idx = 0; idx < _tb.getCapacity(); idx++) {
		double _tmp_frac_ =		
		((double) _tb.getIntegeAtIdx(idx) / (double) _sampledRecordNumber_);
		out.writeBytes(_iter_outside_ + ToolBox.hiveDelimiter
			+ _tb.getStringAtIdx(idx) + ToolBox.hiveDelimiter
			+ _tb.getIntegeAtIdx(idx) + ToolBox.hiveDelimiter
			+ _tmp_frac_ + "\n");
	    }
	}

    }

    void flushHistogram(FSDataOutputStream out) throws Exception {
	out.writeBytes(HistogramOperator.HISTOGRAMTABLE + "\n");
	for (String _s : mcvList.keySet()) {
	    out.writeBytes(_s + "\n");
	    ToolBox _tb = new ToolBox();
	    TreeMap<String, Integer> _tsi = mcvList.get(_s);
	    for (String _s_inner_ : _tsi.keySet()) {
		_tb.push(_s_inner_, _tsi.get(_s_inner_));
	    }
	    // histogram
	    ToolBox _copyBox = HistogramOperator.binning(_tb, 10);
	    String _curString = null;
	    String _preString = _copyBox.getStringAtIdx(0);
	    int idx;
	    for (idx = 1; idx < _copyBox.getCapacity(); idx++) {
		_curString = _copyBox.getStringAtIdx(idx);
		if (_curString.equals(_preString)) {
		    continue;
		} else {
		    out.writeBytes(_copyBox.getIntegeAtIdx(idx - 1)
			    + ToolBox.hiveDelimiter + _s
			    + ToolBox.hiveDelimiter
			    + _copyBox.getStringAtIdx(idx - 1) + "\n");

		    _preString = _curString;
		}
	    }

	    out.writeBytes(_copyBox.getIntegeAtIdx(idx - 1)
		    + ToolBox.hiveDelimiter + _s + ToolBox.hiveDelimiter
		    + _copyBox.getStringAtIdx(idx - 1) + "\n");

	}

    }

    void flushDistinctValue(FSDataOutputStream out) throws Exception {
	out.writeBytes("DistinctValue" + "\n");
	for (String _s : mcvList.keySet()) {
	    double _result = ToolBox.calDistincValue(mcvList.get(_s), stat_num_records);
	    String _surfix = _s.substring(HistogramOperator.MCVLIST_ATTR.length());
	    String _finalString = "DistinctValue" + _surfix;
	    out.writeBytes(_finalString + ToolBox.hiveDelimiter + _result
		    + "\n");

	}
    }

    void flushTableStatsInfo(FSDataOutputStream out) throws Exception {
	// System.out.println("[Trace] cbr.flushTableStatsInfo");
	// System.out.println("[Trace] cbr.flushTableStatsInfo stat_num_records is "
	// + stat_num_records);
	//
	out.writeBytes("TableInformation\n");
	out.writeBytes("stat_num_records" + ToolBox.hiveDelimiter + stat_num_records + "\n");
	ArrayList<TreeMap<String, Integer>> _a = ToolBox.<Integer> aggregateKey(infoDict, ToolBox.hiveDelimiter, 2);
	

	for (TreeMap<String, Integer> treeMap : _a) {
	    for (String _s : treeMap.keySet()) {
		out.writeBytes(_s + ToolBox.hiveDelimiter + treeMap.get(_s)
			+ "\n");

		if (_s.startsWith(StatsCollectionOperator.FIELDLENGTH_ATTR)) {
		    double _tmp = (double) treeMap.get(_s)
			    / (double) stat_num_records;
		    String _avg = StatsCollectionOperator.AVGFIELDWIDTH
			    + _s.substring(StatsCollectionOperator.FIELDLENGTH_ATTR.length());
		    out.writeBytes(_avg + ToolBox.hiveDelimiter + (long) _tmp
			    + "\n");
		}

		if (_s.startsWith(StatsCollectionOperator.NULLCOUNTER_ATTR)) {
		    double _tmp = (double) treeMap.get(_s)
			    / (double) stat_num_records;
		    String _avg = StatsCollectionOperator.NULLFAC_ATTR
			    + _s.substring(StatsCollectionOperator.NULLCOUNTER_ATTR.length());
		    out.writeBytes(_avg + ToolBox.hiveDelimiter + _tmp + "\n");
		}
	    }
	}
	// System.out.println("[Trace] End of cbr.flushTableStatsInfo");
    }

    void getFileSystemStats(Path uri) throws IOException {
	FileStatus _ax = _fs.getFileStatus(uri);
	if (_ax.isDir()) {
	    FileStatus[] _fsArr = _fs.listStatus(uri);
	    for (FileStatus obj : _fsArr) {
		getFileSystemStats(obj.getPath());
	    }
	} else {
	    // _ax is a file here;
	    ContentSummary _cs = _fs.getContentSummary(uri);
	    stat_num_files += _cs.getFileCount();
	    stat_total_size += _cs.getLength();
	    stat_num_blocks += _fs.getFileBlockLocations(_fs.getFileStatus(uri), 0, stat_total_size).length;

	}
    }

    void flushInfo_DFS(FSDataOutputStream _out) throws Exception {
	assert (_out != null);

	_out.writeBytes("DFSInformation\n");
	// System.out.println("[TRACE] The _tableDataLocation_ is " +
	// _tableDataLocation_);
	Path tablePath = new Path(_tableDataLocation_);
	stat_num_files = 0;
	stat_total_size = 0;
	stat_num_blocks = 0;
	getFileSystemStats(tablePath);
	// System.out.println("[TRACE] The stat_total_size is " +
	// stat_total_size);
	// System.out.println("[TRACE] The stat_num_files is " +
	// stat_num_files);
	// System.out.println("[TRACE] The stat_num_blocks is " +
	// stat_num_blocks);

	_out.writeBytes("stat_total_size" + ToolBox.hiveDelimiter
		+ _finalTblName_ + ToolBox.hiveDelimiter + stat_total_size
		+ "\n");
	_out.writeBytes("stat_num_files" + ToolBox.hiveDelimiter
		+ _finalTblName_ + ToolBox.hiveDelimiter + stat_num_files
		+ "\n");
	_out.writeBytes("stat_num_blocks" + ToolBox.hiveDelimiter
		+ _finalTblName_ + ToolBox.hiveDelimiter + stat_num_blocks
		+ "\n");
    }

    void parseTableStruct() {
	// System.out.println("[Trace]CBR.parseTableStruct");
	// using comma

	String _hName_ = jc.get(ToolBox.TABLE_HEADER_NAMES_ATTR);
	if (_hName_ == null) {
	    return;
	}
	StringTokenizer _stName_ = new StringTokenizer(_hName_, ToolBox.commaDelimiter);
	String _hType_ = jc.get(ToolBox.TABLE_HEADER_TYPES_ATTR);
	StringTokenizer _stType_ = new StringTokenizer(_hType_, ToolBox.commaDelimiter);
	while (_stName_.hasMoreTokens() && _stType_.hasMoreTokens()) {
	    tblStructHashMap.put(_stName_.nextToken(), _stType_.nextToken());

	}
    }

}
