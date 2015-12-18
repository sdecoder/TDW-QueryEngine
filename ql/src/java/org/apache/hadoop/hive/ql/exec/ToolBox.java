package org.apache.hadoop.hive.ql.exec;

import java.awt.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.statsDesc;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.join.Parser.Node;

public class ToolBox {
    public final static String dotDelimiter = ".";
    public final static String colonDelimiter = ":";
    public final static String blankDelimiter = " ";
    public final static String tabDelimiter = "\t";
    public final static String commaDelimiter = ",";
    public final static String starDelimiter = "*";
    public final static String hiveDelimiter = "\1";

    public final static String CBR_SWITCH_ATTR = "Cb.Switch";
    public final static String CB_OPT_ATTR = "Cb.Optimization";
    public final static String STATISTICS_COLUMNS_ATTR = "statistics.columns";
    public final static String TABLE_HEADER_NAMES_ATTR = "TABLEHEADERNAMESATTR";
    public final static String TABLE_HEADER_TYPES_ATTR = "TABLEHEADERTYPESATTR";

    public enum SortMethod {
	AscendSort, DescendSort
    };

    SortMethod _sortMethod;
    ArrayList<Tuple> _l;

    public class Tuple {
	String _value;
	Integer _freq;

	public Tuple(String v, Integer i) {
	    this._value = v;
	    this._freq = i;
	}

	public Tuple() {
	}

	public String getString() {
	    return this._value;
	}

	public Integer getInteger() {
	    return this._freq;
	}
    }

    public class TupleComparatorAscend implements Comparator<Tuple> {
	public int compare(Tuple t1, Tuple t2) {
	    int res = t1.getInteger().compareTo(t2.getInteger());
	    if (res == 0) {
		if (t1.getString().compareTo(t2.getString()) < 0) {
		    res = -1;
		}
	    }
	    if (res == 0) {
		res = t1.hashCode() - t2.hashCode();
	    }
	    return res;
	}
    }

    public class TupleComparatorDescend implements Comparator<Tuple> {
	public int compare(Tuple t1, Tuple t2) {
	    int res = t1.getInteger().compareTo(t2.getInteger());
	    if (res == 0) {
		if (t1.getString().compareTo(t2.getString()) < 0) {
		    res = -1;
		}
	    }
	    if (res == 0) {
		res = t1.hashCode() - t2.hashCode();
	    }
	    return -res;
	}
    }

    public ToolBox() {
	_l = new ArrayList<Tuple>();
    }

    public void push(String value, Integer fre) {
	Tuple _t = new Tuple(value, fre);
	_l.add(_t);
    }

    public void ascendSort() {
	Comparator<Tuple> comparator = new TupleComparatorAscend();
	Collections.sort(_l, comparator);
    }

    public void descendSort() {
	Comparator<Tuple> comparator = new TupleComparatorDescend();
	Collections.sort(_l, comparator);
    }

    public int getCapacity() {
	return _l.size();
    }

    public String getStringAtIdx(int _idx) {
	return _l.get(_idx).getString();
    }

    public Integer getIntegeAtIdx(int _idx) {
	return _l.get(_idx).getInteger();
    }

    void compact(java.util.Map<String, Integer> _para, SortMethod _sm,
	    final Object _o) {
	_l.clear();
	for (String _key : _para.keySet()) {
	    push(_key, _para.get(_key));
	}

	if (_sm == ToolBox.SortMethod.AscendSort) {
	    ascendSort();
	} else {
	    assert (_sm == ToolBox.SortMethod.DescendSort);
	    descendSort();
	}

	// _o.getClass()
	if (_o.getClass().getName().equalsIgnoreCase(Double.class.getName())) {
	    double tailFactor = ((Double) _o).doubleValue();
	    _para.clear();
	    for (int idx = 0; idx < (int) (getCapacity() * tailFactor); idx++) {
		_para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
	    }
	} else {
	    assert (_o.getClass().getName().equalsIgnoreCase(Integer.class.getName()));
	    int tailFactor = ((Integer) _o).intValue();
	    _para.clear();
	    for (int idx = 0; idx < tailFactor; idx++) {
		_para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
	    }
	}

    }

    void compactByAscendSort(java.util.Map<String, Integer> _para,
	    final double tailFactor) {
	// we only retain 80% here;
	_l.clear();
	for (String _key : _para.keySet()) {
	    push(_key, _para.get(_key));
	}
	ascendSort();
	_para.clear();
	for (int idx = 0; idx < (int) (getCapacity() * tailFactor); idx++) {
	    _para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
	}
    }

    void compactByDescendSort(java.util.Map<String, Integer> _para,
	    final double tailFactor) {
	// we only retain 80% here;
	_l.clear();
	for (String _key : _para.keySet()) {
	    push(_key, _para.get(_key));
	}
	descendSort();
	_para.clear();
	for (int idx = 0; idx < (int) (getCapacity() * tailFactor); idx++) {
	    _para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
	}
    }

    static HiveKey getHiveKey(String key, byte streamTag) {
	HiveKey keyWritable = new HiveKey();
	int keylen = key.length();
	keyWritable.setSize(keylen + 1);
	System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keylen);
	keyWritable.get()[keylen] = streamTag;
	final int r = 0;
	// for (int i = 0; i < key.length(); i++) {
	// r = r * 31 + (int) key.getBytes()[i];
	// }
	// l4j.debug("[Debug] HiveKey: " + keyWritable.toString());
	// l4j.debug("[Debug] HiveKey.setHashCode: " + r);
	keyWritable.setHashCode(r);
	return keyWritable;
    }

    static String getOriginalKey(BytesWritable key) {

	byte[] _b = key.getBytes();
	return new String(_b, 0, key.getSize() - 1);
    }

    static String retrieveComponent(String _s, String _d, int idx) {
	StringTokenizer _st_ = new StringTokenizer(_s, _d);
	String _return = null;
	for (int i = 0; i < idx; i++) {
	    _return = _st_.nextToken();
	}
	return _return;
    }

    static <T> ArrayList<TreeMap<String, T>> aggregateKey(
	    TreeMap<String, T> _para, String delimiter, int idx) {
	// we don't care what is the key
	ArrayList<TreeMap<String, T>> _a = new ArrayList<TreeMap<String, T>>();
	String _prekey = null;
	TreeMap<String, T> _h = null;
	for (String _s : _para.keySet()) {
	    if (_prekey == null) {
		_prekey = retrieveComponent(_s, delimiter, idx);
		_h = new TreeMap<String, T>();
		_h.put(_s, _para.get(_s));
	    } else if (_prekey.equals(_s)) {
		_h.put(_s, _para.get(_s));
	    } else {
		_prekey = retrieveComponent(_s, delimiter, idx);
		;
		_a.add(_h);
		_h = new TreeMap<String, T>();
		_h.put(_s, _para.get(_s));
	    }

	}
	_a.add(_h);
	return _a;
    }

    @Deprecated
    static ArrayList<TreeMap<String, String>> aggregateKey_string(
	    TreeMap<String, String> _para, String delimiter, int idx) {
	// aggregate using the idx field
	ArrayList<TreeMap<String, String>> _a = new ArrayList<TreeMap<String, String>>();
	String _prekey = null;
	TreeMap<String, String> _h = null;
	for (String _s : _para.keySet()) {
	    if (_prekey == null) {
		_prekey = retrieveComponent(_s, delimiter, idx);
		_h = new TreeMap<String, String>();
		_h.put(_s, _para.get(_s));
	    } else if (_prekey.equals(_s)) {
		_h.put(_s, _para.get(_s));
	    } else {
		_prekey = retrieveComponent(_s, delimiter, idx);
		;
		_a.add(_h);
		_h = new TreeMap<String, String>();
		_h.put(_s, _para.get(_s));
	    }

	}
	_a.add(_h);
	return _a;
    }

    @Deprecated
    static ArrayList<TreeMap<String, Integer>> aggregateKey_Integer(
	    TreeMap<String, Integer> _para, String delimiter, int idx) {
	// aggregate using the idx field
	ArrayList<TreeMap<String, Integer>> _a = new ArrayList<TreeMap<String, Integer>>();
	String _prekey = null;
	TreeMap<String, Integer> _h = null;
	for (String _s : _para.keySet()) {
	    if (_prekey == null) {
		_prekey = retrieveComponent(_s, delimiter, idx);
		_h = new TreeMap<String, Integer>();
		_h.put(_s, _para.get(_s));
	    } else if (_prekey.equals(_s)) {
		_h.put(_s, _para.get(_s));
	    } else {
		_prekey = retrieveComponent(_s, delimiter, idx);
		;
		_a.add(_h);
		_h = new TreeMap<String, Integer>();
		_h.put(_s, _para.get(_s));
	    }

	}
	_a.add(_h);
	return _a;
    }

    static double calDistincValue(TreeMap<String, Integer> _para,
	    int num_sampled_rows) {
	// ��������ֵ��HashTable��ͳ�Ƴ��ִ�������1�ļ�ֵnum_multiple��
	// ͳ�Ƴ���ֵHashTable��Distinctֵ�ĸ���num_distinct��
	// ���num_multiple ==
	// 0����ô˵�����ֶ������ű���û���ظ����ֵļ�ֵ��Ϊunique�ֶΡ���ô��stat_distinct_valuesΪ-1��
	// ���num_multiple ==
	// num_distinct����ô˵�����ֶε�����ֵ�����ű��ж��������������ϣ�ֻ�ǹ���ֵ������ȷ������ô��stat_distinct_valuesΪnum_distinct��
	// ʹ��Haas��Stokes��IBM Research Report RJ 10025���㷨����distinctֵ����n * d / (n
	// - f1 + f1 * n / N)��
	// ���У�NΪ�ܼ�¼����nΪ�����ļ�¼����f1Ϊ��n�������ļ�¼��ֻ����һ�ε�distinct����dΪ��n�������ļ�¼����distinct��������
	// ����f1 = num_distinct - num_multiple��
	// ����d = num_distinct��
	// ����һ��distinct_values��
	// numer = num_sampled_rows * d;
	// denom = (num_sampled_rows - f1) + f1 * num_sampled_rows / totalrows;
	// distinct_values = numer / denom;
	// ���distinct_valuesС��d����ôdistinct_values����Ϊd��
	// ���distinct_values����totalrows����ôdistinct_values����Ϊtotalrows;
	// ���Ƶ�stat_distinct_valuesΪfloor(distinct_values + 0.5)
	// ������Ƶ�stat_distinct_values�������ܼ�¼����10%����ô������Ϊ���ֶε��ǽ���ɢ���ȵģ���¼����ɢ��stat_distinct_values
	// = -(stat_distinct_values / totoalrows)��
	//
	// ��������ֵ�б�[McvList done]
	// ���ͳ�Ƶõ��ĳ���ֵ����(cnt_mcv)С���û�ָ��Ԫ�����б���ĳ���ֵ����num_mcv������cnt_mcv ==
	// num_distinct��stat_distinct_values > 0����ôͳ�Ƶĳ���ֵ�����Է���Ԫ�����У�num_mcv =
	// cnt_mcv��
	// ���򣬼��㳣��ֵ�б�
	// ���Ʋ�����¼�У�ƽ��ÿ��ֵ���ֵĴ�����avg_count = num_sampled_rows /
	// stat_distinct_values
	// �����stat_distinct_valuesС��0����Ϊ-stat_distinct_values * totalrows����
	// ���㳣��ֵ����С���ִ�����min_count = avg_count * 1.25������С���ִ���ҪΪ2�����ϣ�1�εľ�������¼��
	// ����������¼������num_mcv�����ִ�������min_count�ĳ���ֵ�����ɳ���ֵ�б������ճ��ִ����Ӹߵ��ͽ��б�����
	int num_multiple = 0;
	int num_distinct = _para.keySet().size();
	double stat_distinct_values;
	for (String _s_ : _para.keySet()) {
	    if (_para.get(_s_) > 1) {
		num_multiple++;
	    }
	}
	if (num_multiple == 0) {
	    stat_distinct_values = -1;
	} else if (num_multiple == num_distinct) {
	    stat_distinct_values = num_distinct;
	}
	int totalrows = num_sampled_rows;
	int f1 = num_distinct - num_multiple;
	int d = num_distinct;
	int numer = num_sampled_rows * d;
	int denom = (num_sampled_rows - f1) + f1 * num_sampled_rows / totalrows;
	int distinct_values = numer / denom;
	if (distinct_values < d) {
	    distinct_values = d;
	} else if (distinct_values > totalrows) {
	    distinct_values = totalrows;
	}

	stat_distinct_values = Math.floor(distinct_values + 0.5);
	if (stat_distinct_values > 0.1 * totalrows) {
	    stat_distinct_values = -(stat_distinct_values / totalrows);
	}

	return stat_distinct_values;
    }

    static String convertHivePrimitiveStringToLazyTypeString(
	    String _hivePrimitive) {
	String _lazyType = null;
	// null denotes failure;
	if (_hivePrimitive.equalsIgnoreCase("STRING"))
	    _lazyType = "LazyString";
	else if (_hivePrimitive.equalsIgnoreCase("BIGINT"))
	    _lazyType = "LazyInteger";
	else if (_hivePrimitive.equalsIgnoreCase("INT"))
	    _lazyType = "LazyInteger";
	else if (_hivePrimitive.equalsIgnoreCase("TINYINT"))
	    _lazyType = "LazyByte";
	else if (_hivePrimitive.equalsIgnoreCase("DOUBLE"))
	    _lazyType = "LazyDouble";
	else if (_hivePrimitive.equalsIgnoreCase("FLOAT"))
	    _lazyType = "LazyFloat";
	else if (_hivePrimitive.equalsIgnoreCase("BOOLEAN"))
	    _lazyType = "LazyBoolean";
	return _lazyType;
    }

    static String convertLazyObjectToString(Object value) {
	String _className = value.getClass().getName();
	if (_className.endsWith("LazyByte")) {
	    byte _byte = ((LazyByteObjectInspector) LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(PrimitiveCategory.BYTE, false, (byte) 0)).get(value);
	    return String.valueOf(_byte);
	}
	if (_className.endsWith("LazyInteger")) {
	    int _int = ((LazyIntObjectInspector) LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(PrimitiveCategory.INT, false, (byte) 0)).get(value);
	    return String.valueOf(_int);
	}
	if (_className.endsWith("LazyLong")) {
	    long _long = ((LazyLongObjectInspector) LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(PrimitiveCategory.LONG, false, (byte) 0)).get(value);
	    return String.valueOf(_long);
	}
	if (_className.endsWith("LazyFloat")) {
	    float _float = ((LazyFloatObjectInspector) LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(PrimitiveCategory.FLOAT, false, (byte) 0)).get(value);
	    return String.valueOf(_float);
	}

	if (_className.endsWith("LazyDouble")) {
	    double _double = ((LazyDoubleObjectInspector) LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(PrimitiveCategory.DOUBLE, false, (byte) 0)).get(value);
	    return String.valueOf(_double);
	}

	if (_className.endsWith("LazyString")) {
	    return ((LazyString) value).getWritableObject().toString();

	}
	return new String("");
	/*
	 * case STRING: return getLazyStringObjectInspector(escaped,
	 * escapeChar);
	 */
    }

    public static void debugNode(org.apache.hadoop.hive.ql.lib.Node _para,
	    int indent) {
	if (_para.getChildren() == null) {
	    return;
	}
	for (org.apache.hadoop.hive.ql.lib.Node _node : _para.getChildren()) {
	    for (int idx = 0; idx < indent; idx++) {
		System.out.print(" ");

	    }/*
	      * if (expressionTree.getChild(0).getType() ==
	      * HiveParser.Identifier) { String functionName =
	      * unescapeIdentifier(expressionTree.getChild(0).getText()); if
	      * (FunctionRegistry.getGenericUDAFResolver(functionName) != null)
	      * { aggregations.put(expressionTree.toStringTree(),
	      * expressionTree); return; } }
	      */
	    // System.out.print("The node is " + _node.getName());
	    // if (_node.getChild(0).getType() == HiveParser.Identifier) {
	    // System.out.println(_node.getChild(0).getText());
	    // }
	    debugNode(_node, indent + 1);
	}

    }

    static public class tableTuple {
	String _tableName_;
	String _fieldName_;

	public tableTuple() {
	    _tableName_ = null;
	    _fieldName_ = null;
	}

	public tableTuple(String _tn, String _fn) {
	    this._tableName_ = _tn;
	    this._fieldName_ = _fn;
	}

	public String getTableName() {
	    return _tableName_;
	}

	public void setTableName(String _tn) {
	    this._tableName_ = _tn;
	}

	public String getFieldName() {
	    return _fieldName_;
	}

    }

    static public class tableAliasTuple {
	String _tableName_;
	String _alias_;

	public tableAliasTuple(String _tn, String _al) {
	    this._tableName_ = _tn;
	    this._alias_ = _al;
	}

	public tableAliasTuple() {

	}

	public String getTableName() {
	    return _tableName_;
	}

	public String getAlias() {
	    return _alias_;
	}

    }

    static public class tableDistinctTuple {
	String _tableName_;
	String _distinctField_;

	public tableDistinctTuple(String _tn, String _df) {
	    this._tableName_ = _tn;
	    this._distinctField_ = _df;
	}

	public tableDistinctTuple() {

	}

	public String getTableName() {
	    return _tableName_;
	}

	public String getDistinctField() {
	    return _distinctField_;
	}
    }

    

}
