package org.apache.hadoop.hive.ql.exec;

//~--- non-JDK imports --------------------------------------------------------

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.tools.sysinfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities.streamStatus;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.statsDesc;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyShortObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyVoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

//~--- JDK imports ------------------------------------------------------------

import java.io.IOException;
import java.io.Serializable;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import javax.tools.Tool;

//TODO: inferface to the offline analyze

public class StatsCollectionOperator extends Operator<statsDesc> implements
	Serializable {
    // ETL Tool configuration
    private static final long serialVersionUID = 1L;
    public static final byte streamTag = (byte) 0xf0;

    public final String STATISTICS_FIELDS_ATTR = "StatisticsFieldsSwitch";
    // "statistics.columns" = "col1,col2,col3,...,coln"
    // "statistics.sampling.factor" = "20%"

    public static final String FIELDLENGTH_ATTR = "FieldLength";
    public static final String AVGFIELDWIDTH = "StatAvgFieldWidth";
    public static final String NULLCOUNTER_ATTR = "NullCounter";
    public static final String NULLFAC_ATTR = "StatNullFac";
    public static final String RECORDSNUM_ATTR = "StatNumRecords";
    public static final String STATSDISTINCTVALUES_ATTR = "StatDistinctValues";
    public static final String NUMDISTINCT_ATTR = "NumDistinct";
    public static final String MCVTABLETHRESHOLD_ATTR = "MCVTableThreshold"; // k
    public static final String VALFRE_ATTR = "ValFre";
    public static final String UNITSNUM_ATTR = "UnitSum";

    // Watcher
    public static final Log l4j = LogFactory.getLog("StatsCollectionOperator");

    final String DISTINCTTHRESHOLDPERCENTAGE_ATTR = "distinctThresholdPercent";

    // ModeEnum _mode;
    ArrayList<String> _als;
    ArrayList<exprNodeDesc> _aend;
    ArrayList<String> _outputFiledNames_;
    int distinctThresholdPercent;
    HashMap<String, Integer> filterTable;
    HashMap<String, Integer> statsInfo;
    HashSet<Integer> _switcHashSet;

    boolean selStar;

    int stat_avg_field_width;
    int stat_distinct_values;
    int mcvTableThresholdValue;
    // Field level info collection;
    int stat_nullfac;
    // int stat_num_blocks;

    // File system section;
    int stat_num_files;

    // Table level info collection;
    int stat_num_records;
    // int stat_num_units;
    int performanceCounter;
    String tableName;
    StringBuilder wholeKeyString;

    // used for pipeline opt;
    transient protected ExprNodeEvaluator[] eval;
    transient Object[] output;

    public void process(Object row, int tag) throws HiveException {
	stat_num_records++;
	try {

	    for (int i = 0; i < eval.length; i++) {
		try {
		    output[i] = eval[i].evaluate(row);
		} catch (HiveException e) {
		    e.printStackTrace();
		} catch (RuntimeException e) {
		    e.printStackTrace();
		}

	    }
	    forward(output, outputObjInspector);
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    /**
     * @return the name of the operator
     */
    public String getName() {
	return new String("STATS");
    }

    public void closeOp(boolean abort) throws HiveException {
	//System.out.println("[Trace] StatsCollectionOperator.closeOp: Initialized");
	try {
	    wholeKeyString.delete(0, wholeKeyString.length());
	    wholeKeyString.append(StatsCollectionOperator.RECORDSNUM_ATTR);
	    wholeKeyString.append(ToolBox.hiveDelimiter);
	    wholeKeyString.append(conf.getTableName());
	    HiveKey _outputKey = ToolBox.getHiveKey(wholeKeyString.toString(), this.streamTag);
	    out.collect(_outputKey, new Text(String.valueOf(stat_num_records)));
	} catch (Exception e) {
	    e.printStackTrace();
	}
	//System.out.println("[Trace] StatsCollectionOperator.closeOp: Terminated");
    }

    protected void initializeOp(Configuration hconf) throws HiveException {
	//System.out.println("[Trace]statsCollectionOp.initializeOp: Initialized");
	super.initializeOp(hconf);

	if (conf == null) {
	    //System.out.println("[Trace]statsCollectionOp.initializeOp conf is null, return");
	    return;
	}

	this.tableName = conf.getTableName();
	assert this.tableName != null;

	this.distinctThresholdPercent = conf.getDistinctThresholdPercent();
	this.mcvTableThresholdValue = conf.getMcvTableThresholdValue();
	this.selStar = conf.getSelStar();

	this.stat_avg_field_width = 0;
	this.stat_distinct_values = 0;
	this.stat_nullfac = 0;
	this.stat_num_files = 0;
	this.stat_num_records = 0;

	performanceCounter = 0;
	wholeKeyString = new StringBuilder();
	filterTable = new HashMap<String, Integer>();
	statsInfo = new HashMap<String, Integer>();

	_switcHashSet = new HashSet<Integer>();
	_outputFiledNames_ = new ArrayList<String>();

	_als = conf.getFieldNames();
	if (_als == null) {
	    System.out.println("[ERROR] Fetch _als[getFieldNames()] failed. Operator aborted.");
	    throw new HiveException("[ERROR] Fetch _als[getFieldNames()] failed. Operator aborted.");
	}

	ArrayList<Integer> _ari = conf.getFieldStatsSwitch();
	for (Integer _i : _ari) {
	    _switcHashSet.add(_i);
	    _outputFiledNames_.add(_als.get(_i));
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
	outputObjInspector = initEvaluatorsAndReturnStruct(eval, _outputFiledNames_, inputObjInspectors[0]);

	//System.out.println("[Trace]statsCollectionOp.initializeOp: Terminated");

    }

}
