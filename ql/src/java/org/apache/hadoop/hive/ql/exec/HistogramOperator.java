package org.apache.hadoop.hive.ql.exec;

//import hadoop sdk;
import org.apache.derby.iapi.services.io.NewByteArrayInputStream;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.tools.sysinfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.histogramDesc;
import org.apache.hadoop.hive.ql.plan.sampleDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

//import java sdk;
import java.text.Bidi;
import java.util.*;
import java.awt.RenderingHints.Key;
import java.io.Serializable;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;

import javax.print.attribute.standard.NumberOfDocuments;
import javax.print.attribute.standard.MediaSize.NA;

// buffer incoming data from sampler operator;

public class HistogramOperator extends Operator<histogramDesc> implements
	Serializable {
    public final static byte streamTag = (byte) 0xf2;
    public final static String STATISTIC_HISTOGRAM_SIZE_ATTR = "StatisticsHistogramSize";
    public final static String MCVLIST_ATTR = "Mcvlist";
    public final static String HISTOGRAMTABLE = "HistogramTable";

    String destFilePath;
    String _tblName_;
    StringBuilder _hStringBuilder;
    ArrayList<Integer> _switch_;
    ArrayList<String> _targetFldNames_;
    ArrayList<String> _AllFldNames_;
    HashMap<String, HashMap<String, Integer>> mcvList;
    HashMap<String, Integer> _statsInfoDict_;
    java.util.ArrayList<exprNodeDesc> _aend;

    transient protected ExprNodeEvaluator[] eval;
    transient Object[] output;
    long millisecCounter;

    ToolBox _tb;
    int statsHisgramSzie;
    JobConf currentConf;
    int processCounter = 0;
    long timeReg = 0;
    long timeSec = 0;

    protected void initializeOp(Configuration hconf) throws HiveException {
	// you must decide this split should be sampled or not;
	//System.out.println("[Trace]Histogram.initializeOp");
	super.initializeOp(hconf);

	millisecCounter = 0;
	_hStringBuilder = new StringBuilder();
	mcvList = new HashMap<String, HashMap<String, Integer>>();
	_statsInfoDict_ = new HashMap<String, Integer>();
	_targetFldNames_ = new ArrayList<String>();
	_tb = new ToolBox();

	_tblName_ = conf.getTableName();
	if (_tblName_ == null) {
	    System.out.println("[ERROR] Fetch _tblName_ failed. Operator aborted");
	    throw new HiveException("[ERROR] Fetch _tblName_ failed. Operator aborted");
	}

	_AllFldNames_ = conf.getFieldNames();
	if (_AllFldNames_ == null) {
	    System.out.println("[ERROR] Fetch _AllFldNames_ failed. Operator aborted");
	    throw new HiveException("[ERROR] Fetch _AllFldNames_ failed. Operator aborted");
	}

	_switch_ = conf.getSwitch();
	if (_switch_ == null) {
	    System.out.println("[ERROR] Fetch _switch_ failed. Operator aborted");
	    throw new HiveException("[ERROR] Fetch _switch_ failed. Operator aborted");
	}
	for (Integer ax : _switch_) {
	    _targetFldNames_.add(_AllFldNames_.get(ax));
	}

	_aend = conf.getColList();
	eval = new ExprNodeEvaluator[_aend.size()];
	for (int i = 0; i < _aend.size(); i++) {
	    assert (_aend.get(i) != null);
	    eval[i] = ExprNodeEvaluatorFactory.get(_aend.get(i));
	    System.out.println("The eval " + i + " is a class of "
		    + eval[i].getClass().getName());
	}
	output = new Object[eval.length];
	outputObjInspector = initEvaluatorsAndReturnStruct(eval, _targetFldNames_, inputObjInspectors[0]);

    }

    public void process(Object row, int tag) throws HiveException {
	forward(row, null);
	processCounter++;

	try {

	    Object[] _os = (Object[]) row;
	    for (int idx = 0; idx < _os.length; idx++) {
		Object _f = _os[idx];
		if (_f == null) {
		    // System.out.println("[ERROR] Object retrieved from _field is null");
		    INC(idx, StatsCollectionOperator.NULLCOUNTER_ATTR, 1);

		} else {

		    // output[idx] = eval[idx].evaluate(row);
		    // String _objStr = String.valueOf(output[idx]);
		    //
		    String _objStr = _f.toString();
		    //System.out.println("The objStr is " + _objStr);
		    // String _objStr = ToolBox.convertLazyObjectToString(_f);
		    int fldLength = _objStr.length();
		    INC(idx, StatsCollectionOperator.FIELDLENGTH_ATTR, fldLength); // fldLength

		    //String _objStr_ = ToolBox.convertLazyObjectToString(_f);
		    _hStringBuilder.delete(0, _hStringBuilder.length());
		    _hStringBuilder.append(MCVLIST_ATTR);
		    _hStringBuilder.append(ToolBox.hiveDelimiter);
		    _hStringBuilder.append(_tblName_);
		    _hStringBuilder.append(ToolBox.hiveDelimiter);
		    _hStringBuilder.append(_AllFldNames_.get(_switch_.get(idx)));

		    // System.out.println("[Trace] Histogram.process.mcvlist.key "
		    // + _key_);
		    HashMap<String, Integer> _valfre_ = mcvList.get(_hStringBuilder.toString());
		    if (_valfre_ == null) {
			_valfre_ = new HashMap<String, Integer>();
			mcvList.put(_hStringBuilder.toString(), _valfre_);
		    }
		    Integer _i = _valfre_.get(_objStr);
		    if (_i == null) {
			_i = Integer.valueOf(1);
		    } else {
			_i++;
		    }
		    _valfre_.put(_objStr, _i);
		    if (_valfre_.keySet().size() > 1048576) {
			// _tb.compactByDescendSort(_valfre_, 0.8);
			_tb.compact(_valfre_, ToolBox.SortMethod.DescendSort, Integer.valueOf(512));
		    }
		}

	    }

	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    private void INC(int idx, String property, int value) {
	// System.out.println("\t\t\t[Trace] StatsCollectionOperator.inc entrance");
	String _fldName = _AllFldNames_.get(_switch_.get(idx));
	_hStringBuilder.delete(0, _hStringBuilder.length());
	_hStringBuilder.append(property);
	_hStringBuilder.append(ToolBox.hiveDelimiter);
	_hStringBuilder.append(_tblName_);
	_hStringBuilder.append(ToolBox.hiveDelimiter);
	_hStringBuilder.append(_fldName);

	// String wholeKeyString = property + ToolBox.hiveDelimiter + tableName
	// + ToolBox.hiveDelimiter + _fldName;

	assert (_statsInfoDict_ != null);
	Integer axi = _statsInfoDict_.get(_hStringBuilder.toString());
	if (axi == null) {
	    axi = new Integer(value);
	} else {
	    axi += value;
	}
	_statsInfoDict_.put(_hStringBuilder.toString(), axi);

    }

    public String getName() {
	return new String("HISTOGRAM");
    }

    public void closeOp(boolean abort) throws HiveException {
	//System.out.println("[Trace] In the HistogramOperator.closeOp: Initialized");

	try {
	    if (_statsInfoDict_ == null) {
		System.out.println("_statsInfoDict_ == null");
		return;
	    }
	    for (String key : _statsInfoDict_.keySet()) {
		if (out != null) {
		    HiveKey _outputKey = ToolBox.getHiveKey(key, this.streamTag);
		    out.collect(_outputKey, new Text(String.valueOf(_statsInfoDict_.get(key))));
		}
	    }

	} catch (IOException e) {
	    e.printStackTrace();
	}

	try {
	    try {
		for (String key : mcvList.keySet()) {
		    HashMap<String, Integer> _hm_reg_ = mcvList.get(key);
		    if (_hm_reg_.keySet().size() > 128) {
			_tb.compact(_hm_reg_, ToolBox.SortMethod.DescendSort, Integer.valueOf(128));
		    }
		    for (String _s_key_ : _hm_reg_.keySet()) {

			_hStringBuilder.delete(0, _hStringBuilder.length());
			_hStringBuilder.append(_s_key_);
			_hStringBuilder.append(ToolBox.hiveDelimiter);
			_hStringBuilder.append(_hm_reg_.get(_s_key_));
			out.collect(ToolBox.getHiveKey(key, streamTag), new Text(String.valueOf(_hStringBuilder.toString())));
		    }

		}

	    } catch (IOException ioe) {
		ioe.printStackTrace();
	    }

	} catch (Exception e) {
	    e.printStackTrace();
	}
	//System.out.println("[Trace] In the HistogramOperator.closeOp: Terminated");
    }

    public static ToolBox binning(ToolBox _tb, int _num_of_bins) {

	// _tb: values from the same field are boxed into one bin;
	String curFld = null;
	int counter = 0;
	int summer = 0;
	for (int idx = 0; idx < _tb.getCapacity(); idx++) {
	    summer += _tb.getIntegeAtIdx(idx);

	}
	final int container = summer / _num_of_bins;
	int _fill_ = 0;
	int idx = 0;
	ToolBox _copyBox = new ToolBox();
	for (int i = 0; i < _num_of_bins; i++) {
	    while (_fill_ < container) {
		_fill_ += _tb.getIntegeAtIdx(idx);
		idx++;
		if (idx >= _tb.getCapacity()) {
		    // System.out.println("The idx is going to be out of bounce");
		    break;
		}
	    }
	    if (idx >= _tb.getCapacity()) {
		if (_fill_ >= container) {
		    _copyBox.push(_tb.getStringAtIdx(idx - 1), idx - 1);
		}
		break;
	    }
	    if (_fill_ >= container) {
		_fill_ -= container;
		_copyBox.push(_tb.getStringAtIdx(idx), idx);
	    }

	}

	// System.out.println("[Trace] HistogramOperator.binning");
	// for(int _i_ = 0; _i_ < _copyBox.getCapacity() ; _i_ ++){
	// System.out.println("\t [" + _copyBox.getIntegeAtIdx(_i_) + "] " + "["
	// +_copyBox.getStringAtIdx(_i_) + "]");
	// }
	return _copyBox;

    }

}
