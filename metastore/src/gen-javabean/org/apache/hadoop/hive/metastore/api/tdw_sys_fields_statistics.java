/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.hive.metastore.api;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import org.apache.log4j.Logger;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

public class tdw_sys_fields_statistics implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("tdw_sys_fields_statistics");
  private static final TField STAT_TABLE_NAME_FIELD_DESC = new TField("stat_table_name", TType.STRING, (short)1);
  private static final TField STAT_FIELD_NAME_FIELD_DESC = new TField("stat_field_name", TType.STRING, (short)2);
  private static final TField STAT_NULLFAC_FIELD_DESC = new TField("stat_nullfac", TType.DOUBLE, (short)3);
  private static final TField STAT_AVG_FIELD_WIDTH_FIELD_DESC = new TField("stat_avg_field_width", TType.I32, (short)4);
  private static final TField STAT_DISTINCT_VALUES_FIELD_DESC = new TField("stat_distinct_values", TType.DOUBLE, (short)5);
  private static final TField STAT_VALUES_1_FIELD_DESC = new TField("stat_values_1", TType.STRING, (short)6);
  private static final TField STAT_NUMBERS_1_FIELD_DESC = new TField("stat_numbers_1", TType.STRING, (short)7);
  private static final TField STAT_VALUES_2_FIELD_DESC = new TField("stat_values_2", TType.STRING, (short)8);
  private static final TField STAT_NUMBERS_2_FIELD_DESC = new TField("stat_numbers_2", TType.STRING, (short)9);
  private static final TField STAT_VALUES_3_FIELD_DESC = new TField("stat_values_3", TType.STRING, (short)10);
  private static final TField STAT_NUMBERS_3_FIELD_DESC = new TField("stat_numbers_3", TType.STRING, (short)11);
  private static final TField STAT_NUMBER_1_TYPE_FIELD_DESC = new TField("stat_number_1_type", TType.I32, (short)12);
  private static final TField STAT_NUMBER_2_TYPE_FIELD_DESC = new TField("stat_number_2_type", TType.I32, (short)13);
  private static final TField STAT_NUMBER_3_TYPE_FIELD_DESC = new TField("stat_number_3_type", TType.I32, (short)14);

  private String stat_table_name;
  public static final int STAT_TABLE_NAME = 1;
  private String stat_field_name;
  public static final int STAT_FIELD_NAME = 2;
  private double stat_nullfac;
  public static final int STAT_NULLFAC = 3;
  private int stat_avg_field_width;
  public static final int STAT_AVG_FIELD_WIDTH = 4;
  private double stat_distinct_values;
  public static final int STAT_DISTINCT_VALUES = 5;
  private String stat_values_1;
  public static final int STAT_VALUES_1 = 6;
  private String stat_numbers_1;
  public static final int STAT_NUMBERS_1 = 7;
  private String stat_values_2;
  public static final int STAT_VALUES_2 = 8;
  private String stat_numbers_2;
  public static final int STAT_NUMBERS_2 = 9;
  private String stat_values_3;
  public static final int STAT_VALUES_3 = 10;
  private String stat_numbers_3;
  public static final int STAT_NUMBERS_3 = 11;
  private int stat_number_1_type;
  public static final int STAT_NUMBER_1_TYPE = 12;
  private int stat_number_2_type;
  public static final int STAT_NUMBER_2_TYPE = 13;
  private int stat_number_3_type;
  public static final int STAT_NUMBER_3_TYPE = 14;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean stat_nullfac = false;
    public boolean stat_avg_field_width = false;
    public boolean stat_distinct_values = false;
    public boolean stat_number_1_type = false;
    public boolean stat_number_2_type = false;
    public boolean stat_number_3_type = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(STAT_TABLE_NAME, new FieldMetaData("stat_table_name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_FIELD_NAME, new FieldMetaData("stat_field_name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_NULLFAC, new FieldMetaData("stat_nullfac", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.DOUBLE)));
    put(STAT_AVG_FIELD_WIDTH, new FieldMetaData("stat_avg_field_width", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STAT_DISTINCT_VALUES, new FieldMetaData("stat_distinct_values", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.DOUBLE)));
    put(STAT_VALUES_1, new FieldMetaData("stat_values_1", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_NUMBERS_1, new FieldMetaData("stat_numbers_1", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_VALUES_2, new FieldMetaData("stat_values_2", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_NUMBERS_2, new FieldMetaData("stat_numbers_2", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_VALUES_3, new FieldMetaData("stat_values_3", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_NUMBERS_3, new FieldMetaData("stat_numbers_3", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_NUMBER_1_TYPE, new FieldMetaData("stat_number_1_type", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STAT_NUMBER_2_TYPE, new FieldMetaData("stat_number_2_type", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STAT_NUMBER_3_TYPE, new FieldMetaData("stat_number_3_type", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(tdw_sys_fields_statistics.class, metaDataMap);
  }

  public tdw_sys_fields_statistics() {
  }

  public tdw_sys_fields_statistics(
    String stat_table_name,
    String stat_field_name,
    double stat_nullfac,
    int stat_avg_field_width,
    double stat_distinct_values,
    String stat_values_1,
    String stat_numbers_1,
    String stat_values_2,
    String stat_numbers_2,
    String stat_values_3,
    String stat_numbers_3,
    int stat_number_1_type,
    int stat_number_2_type,
    int stat_number_3_type)
  {
    this();
    this.stat_table_name = stat_table_name;
    this.stat_field_name = stat_field_name;
    this.stat_nullfac = stat_nullfac;
    this.__isset.stat_nullfac = true;
    this.stat_avg_field_width = stat_avg_field_width;
    this.__isset.stat_avg_field_width = true;
    this.stat_distinct_values = stat_distinct_values;
    this.__isset.stat_distinct_values = true;
    this.stat_values_1 = stat_values_1;
    this.stat_numbers_1 = stat_numbers_1;
    this.stat_values_2 = stat_values_2;
    this.stat_numbers_2 = stat_numbers_2;
    this.stat_values_3 = stat_values_3;
    this.stat_numbers_3 = stat_numbers_3;
    this.stat_number_1_type = stat_number_1_type;
    this.__isset.stat_number_1_type = true;
    this.stat_number_2_type = stat_number_2_type;
    this.__isset.stat_number_2_type = true;
    this.stat_number_3_type = stat_number_3_type;
    this.__isset.stat_number_3_type = true;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public tdw_sys_fields_statistics(tdw_sys_fields_statistics other) {
    if (other.isSetStat_table_name()) {
      this.stat_table_name = other.stat_table_name;
    }
    if (other.isSetStat_field_name()) {
      this.stat_field_name = other.stat_field_name;
    }
    __isset.stat_nullfac = other.__isset.stat_nullfac;
    this.stat_nullfac = other.stat_nullfac;
    __isset.stat_avg_field_width = other.__isset.stat_avg_field_width;
    this.stat_avg_field_width = other.stat_avg_field_width;
    __isset.stat_distinct_values = other.__isset.stat_distinct_values;
    this.stat_distinct_values = other.stat_distinct_values;
    if (other.isSetStat_values_1()) {
      this.stat_values_1 = other.stat_values_1;
    }
    if (other.isSetStat_numbers_1()) {
      this.stat_numbers_1 = other.stat_numbers_1;
    }
    if (other.isSetStat_values_2()) {
      this.stat_values_2 = other.stat_values_2;
    }
    if (other.isSetStat_numbers_2()) {
      this.stat_numbers_2 = other.stat_numbers_2;
    }
    if (other.isSetStat_values_3()) {
      this.stat_values_3 = other.stat_values_3;
    }
    if (other.isSetStat_numbers_3()) {
      this.stat_numbers_3 = other.stat_numbers_3;
    }
    __isset.stat_number_1_type = other.__isset.stat_number_1_type;
    this.stat_number_1_type = other.stat_number_1_type;
    __isset.stat_number_2_type = other.__isset.stat_number_2_type;
    this.stat_number_2_type = other.stat_number_2_type;
    __isset.stat_number_3_type = other.__isset.stat_number_3_type;
    this.stat_number_3_type = other.stat_number_3_type;
  }

  @Override
  public tdw_sys_fields_statistics clone() {
    return new tdw_sys_fields_statistics(this);
  }

  public String getStat_table_name() {
    return this.stat_table_name;
  }

  public void setStat_table_name(String stat_table_name) {
    this.stat_table_name = stat_table_name;
  }

  public void unsetStat_table_name() {
    this.stat_table_name = null;
  }

  // Returns true if field stat_table_name is set (has been asigned a value) and false otherwise
  public boolean isSetStat_table_name() {
    return this.stat_table_name != null;
  }

  public String getStat_field_name() {
    return this.stat_field_name;
  }

  public void setStat_field_name(String stat_field_name) {
    this.stat_field_name = stat_field_name;
  }

  public void unsetStat_field_name() {
    this.stat_field_name = null;
  }

  // Returns true if field stat_field_name is set (has been asigned a value) and false otherwise
  public boolean isSetStat_field_name() {
    return this.stat_field_name != null;
  }

  public double getStat_nullfac() {
    return this.stat_nullfac;
  }

  public void setStat_nullfac(double stat_nullfac) {
    this.stat_nullfac = stat_nullfac;
    this.__isset.stat_nullfac = true;
  }

  public void unsetStat_nullfac() {
    this.__isset.stat_nullfac = false;
  }

  // Returns true if field stat_nullfac is set (has been asigned a value) and false otherwise
  public boolean isSetStat_nullfac() {
    return this.__isset.stat_nullfac;
  }

  public int getStat_avg_field_width() {
    return this.stat_avg_field_width;
  }

  public void setStat_avg_field_width(int stat_avg_field_width) {
    this.stat_avg_field_width = stat_avg_field_width;
    this.__isset.stat_avg_field_width = true;
  }

  public void unsetStat_avg_field_width() {
    this.__isset.stat_avg_field_width = false;
  }

  // Returns true if field stat_avg_field_width is set (has been asigned a value) and false otherwise
  public boolean isSetStat_avg_field_width() {
    return this.__isset.stat_avg_field_width;
  }

  public double getStat_distinct_values() {
    return this.stat_distinct_values;
  }

  public void setStat_distinct_values(double stat_distinct_values) {
    this.stat_distinct_values = stat_distinct_values;
    this.__isset.stat_distinct_values = true;
  }

  public void unsetStat_distinct_values() {
    this.__isset.stat_distinct_values = false;
  }

  // Returns true if field stat_distinct_values is set (has been asigned a value) and false otherwise
  public boolean isSetStat_distinct_values() {
    return this.__isset.stat_distinct_values;
  }

  public String getStat_values_1() {
    return this.stat_values_1;
  }

  public void setStat_values_1(String stat_values_1) {
    this.stat_values_1 = stat_values_1;
  }

  public void unsetStat_values_1() {
    this.stat_values_1 = null;
  }

  // Returns true if field stat_values_1 is set (has been asigned a value) and false otherwise
  public boolean isSetStat_values_1() {
    return this.stat_values_1 != null;
  }

  public String getStat_numbers_1() {
    return this.stat_numbers_1;
  }

  public void setStat_numbers_1(String stat_numbers_1) {
    this.stat_numbers_1 = stat_numbers_1;
  }

  public void unsetStat_numbers_1() {
    this.stat_numbers_1 = null;
  }

  // Returns true if field stat_numbers_1 is set (has been asigned a value) and false otherwise
  public boolean isSetStat_numbers_1() {
    return this.stat_numbers_1 != null;
  }

  public String getStat_values_2() {
    return this.stat_values_2;
  }

  public void setStat_values_2(String stat_values_2) {
    this.stat_values_2 = stat_values_2;
  }

  public void unsetStat_values_2() {
    this.stat_values_2 = null;
  }

  // Returns true if field stat_values_2 is set (has been asigned a value) and false otherwise
  public boolean isSetStat_values_2() {
    return this.stat_values_2 != null;
  }

  public String getStat_numbers_2() {
    return this.stat_numbers_2;
  }

  public void setStat_numbers_2(String stat_numbers_2) {
    this.stat_numbers_2 = stat_numbers_2;
  }

  public void unsetStat_numbers_2() {
    this.stat_numbers_2 = null;
  }

  // Returns true if field stat_numbers_2 is set (has been asigned a value) and false otherwise
  public boolean isSetStat_numbers_2() {
    return this.stat_numbers_2 != null;
  }

  public String getStat_values_3() {
    return this.stat_values_3;
  }

  public void setStat_values_3(String stat_values_3) {
    this.stat_values_3 = stat_values_3;
  }

  public void unsetStat_values_3() {
    this.stat_values_3 = null;
  }

  // Returns true if field stat_values_3 is set (has been asigned a value) and false otherwise
  public boolean isSetStat_values_3() {
    return this.stat_values_3 != null;
  }

  public String getStat_numbers_3() {
    return this.stat_numbers_3;
  }

  public void setStat_numbers_3(String stat_numbers_3) {
    this.stat_numbers_3 = stat_numbers_3;
  }

  public void unsetStat_numbers_3() {
    this.stat_numbers_3 = null;
  }

  // Returns true if field stat_numbers_3 is set (has been asigned a value) and false otherwise
  public boolean isSetStat_numbers_3() {
    return this.stat_numbers_3 != null;
  }

  public int getStat_number_1_type() {
    return this.stat_number_1_type;
  }

  public void setStat_number_1_type(int stat_number_1_type) {
    this.stat_number_1_type = stat_number_1_type;
    this.__isset.stat_number_1_type = true;
  }

  public void unsetStat_number_1_type() {
    this.__isset.stat_number_1_type = false;
  }

  // Returns true if field stat_number_1_type is set (has been asigned a value) and false otherwise
  public boolean isSetStat_number_1_type() {
    return this.__isset.stat_number_1_type;
  }

  public int getStat_number_2_type() {
    return this.stat_number_2_type;
  }

  public void setStat_number_2_type(int stat_number_2_type) {
    this.stat_number_2_type = stat_number_2_type;
    this.__isset.stat_number_2_type = true;
  }

  public void unsetStat_number_2_type() {
    this.__isset.stat_number_2_type = false;
  }

  // Returns true if field stat_number_2_type is set (has been asigned a value) and false otherwise
  public boolean isSetStat_number_2_type() {
    return this.__isset.stat_number_2_type;
  }

  public int getStat_number_3_type() {
    return this.stat_number_3_type;
  }

  public void setStat_number_3_type(int stat_number_3_type) {
    this.stat_number_3_type = stat_number_3_type;
    this.__isset.stat_number_3_type = true;
  }

  public void unsetStat_number_3_type() {
    this.__isset.stat_number_3_type = false;
  }

  // Returns true if field stat_number_3_type is set (has been asigned a value) and false otherwise
  public boolean isSetStat_number_3_type() {
    return this.__isset.stat_number_3_type;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case STAT_TABLE_NAME:
      if (value == null) {
        unsetStat_table_name();
      } else {
        setStat_table_name((String)value);
      }
      break;

    case STAT_FIELD_NAME:
      if (value == null) {
        unsetStat_field_name();
      } else {
        setStat_field_name((String)value);
      }
      break;

    case STAT_NULLFAC:
      if (value == null) {
        unsetStat_nullfac();
      } else {
        setStat_nullfac((Double)value);
      }
      break;

    case STAT_AVG_FIELD_WIDTH:
      if (value == null) {
        unsetStat_avg_field_width();
      } else {
        setStat_avg_field_width((Integer)value);
      }
      break;

    case STAT_DISTINCT_VALUES:
      if (value == null) {
        unsetStat_distinct_values();
      } else {
        setStat_distinct_values((Double)value);
      }
      break;

    case STAT_VALUES_1:
      if (value == null) {
        unsetStat_values_1();
      } else {
        setStat_values_1((String)value);
      }
      break;

    case STAT_NUMBERS_1:
      if (value == null) {
        unsetStat_numbers_1();
      } else {
        setStat_numbers_1((String)value);
      }
      break;

    case STAT_VALUES_2:
      if (value == null) {
        unsetStat_values_2();
      } else {
        setStat_values_2((String)value);
      }
      break;

    case STAT_NUMBERS_2:
      if (value == null) {
        unsetStat_numbers_2();
      } else {
        setStat_numbers_2((String)value);
      }
      break;

    case STAT_VALUES_3:
      if (value == null) {
        unsetStat_values_3();
      } else {
        setStat_values_3((String)value);
      }
      break;

    case STAT_NUMBERS_3:
      if (value == null) {
        unsetStat_numbers_3();
      } else {
        setStat_numbers_3((String)value);
      }
      break;

    case STAT_NUMBER_1_TYPE:
      if (value == null) {
        unsetStat_number_1_type();
      } else {
        setStat_number_1_type((Integer)value);
      }
      break;

    case STAT_NUMBER_2_TYPE:
      if (value == null) {
        unsetStat_number_2_type();
      } else {
        setStat_number_2_type((Integer)value);
      }
      break;

    case STAT_NUMBER_3_TYPE:
      if (value == null) {
        unsetStat_number_3_type();
      } else {
        setStat_number_3_type((Integer)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case STAT_TABLE_NAME:
      return getStat_table_name();

    case STAT_FIELD_NAME:
      return getStat_field_name();

    case STAT_NULLFAC:
      return new Double(getStat_nullfac());

    case STAT_AVG_FIELD_WIDTH:
      return new Integer(getStat_avg_field_width());

    case STAT_DISTINCT_VALUES:
      return new Double(getStat_distinct_values());

    case STAT_VALUES_1:
      return getStat_values_1();

    case STAT_NUMBERS_1:
      return getStat_numbers_1();

    case STAT_VALUES_2:
      return getStat_values_2();

    case STAT_NUMBERS_2:
      return getStat_numbers_2();

    case STAT_VALUES_3:
      return getStat_values_3();

    case STAT_NUMBERS_3:
      return getStat_numbers_3();

    case STAT_NUMBER_1_TYPE:
      return new Integer(getStat_number_1_type());

    case STAT_NUMBER_2_TYPE:
      return new Integer(getStat_number_2_type());

    case STAT_NUMBER_3_TYPE:
      return new Integer(getStat_number_3_type());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case STAT_TABLE_NAME:
      return isSetStat_table_name();
    case STAT_FIELD_NAME:
      return isSetStat_field_name();
    case STAT_NULLFAC:
      return isSetStat_nullfac();
    case STAT_AVG_FIELD_WIDTH:
      return isSetStat_avg_field_width();
    case STAT_DISTINCT_VALUES:
      return isSetStat_distinct_values();
    case STAT_VALUES_1:
      return isSetStat_values_1();
    case STAT_NUMBERS_1:
      return isSetStat_numbers_1();
    case STAT_VALUES_2:
      return isSetStat_values_2();
    case STAT_NUMBERS_2:
      return isSetStat_numbers_2();
    case STAT_VALUES_3:
      return isSetStat_values_3();
    case STAT_NUMBERS_3:
      return isSetStat_numbers_3();
    case STAT_NUMBER_1_TYPE:
      return isSetStat_number_1_type();
    case STAT_NUMBER_2_TYPE:
      return isSetStat_number_2_type();
    case STAT_NUMBER_3_TYPE:
      return isSetStat_number_3_type();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof tdw_sys_fields_statistics)
      return this.equals((tdw_sys_fields_statistics)that);
    return false;
  }

  public boolean equals(tdw_sys_fields_statistics that) {
    if (that == null)
      return false;

    boolean this_present_stat_table_name = true && this.isSetStat_table_name();
    boolean that_present_stat_table_name = true && that.isSetStat_table_name();
    if (this_present_stat_table_name || that_present_stat_table_name) {
      if (!(this_present_stat_table_name && that_present_stat_table_name))
        return false;
      if (!this.stat_table_name.equals(that.stat_table_name))
        return false;
    }

    boolean this_present_stat_field_name = true && this.isSetStat_field_name();
    boolean that_present_stat_field_name = true && that.isSetStat_field_name();
    if (this_present_stat_field_name || that_present_stat_field_name) {
      if (!(this_present_stat_field_name && that_present_stat_field_name))
        return false;
      if (!this.stat_field_name.equals(that.stat_field_name))
        return false;
    }

    boolean this_present_stat_nullfac = true;
    boolean that_present_stat_nullfac = true;
    if (this_present_stat_nullfac || that_present_stat_nullfac) {
      if (!(this_present_stat_nullfac && that_present_stat_nullfac))
        return false;
      if (this.stat_nullfac != that.stat_nullfac)
        return false;
    }

    boolean this_present_stat_avg_field_width = true;
    boolean that_present_stat_avg_field_width = true;
    if (this_present_stat_avg_field_width || that_present_stat_avg_field_width) {
      if (!(this_present_stat_avg_field_width && that_present_stat_avg_field_width))
        return false;
      if (this.stat_avg_field_width != that.stat_avg_field_width)
        return false;
    }

    boolean this_present_stat_distinct_values = true;
    boolean that_present_stat_distinct_values = true;
    if (this_present_stat_distinct_values || that_present_stat_distinct_values) {
      if (!(this_present_stat_distinct_values && that_present_stat_distinct_values))
        return false;
      if (this.stat_distinct_values != that.stat_distinct_values)
        return false;
    }

    boolean this_present_stat_values_1 = true && this.isSetStat_values_1();
    boolean that_present_stat_values_1 = true && that.isSetStat_values_1();
    if (this_present_stat_values_1 || that_present_stat_values_1) {
      if (!(this_present_stat_values_1 && that_present_stat_values_1))
        return false;
      if (!this.stat_values_1.equals(that.stat_values_1))
        return false;
    }

    boolean this_present_stat_numbers_1 = true && this.isSetStat_numbers_1();
    boolean that_present_stat_numbers_1 = true && that.isSetStat_numbers_1();
    if (this_present_stat_numbers_1 || that_present_stat_numbers_1) {
      if (!(this_present_stat_numbers_1 && that_present_stat_numbers_1))
        return false;
      if (!this.stat_numbers_1.equals(that.stat_numbers_1))
        return false;
    }

    boolean this_present_stat_values_2 = true && this.isSetStat_values_2();
    boolean that_present_stat_values_2 = true && that.isSetStat_values_2();
    if (this_present_stat_values_2 || that_present_stat_values_2) {
      if (!(this_present_stat_values_2 && that_present_stat_values_2))
        return false;
      if (!this.stat_values_2.equals(that.stat_values_2))
        return false;
    }

    boolean this_present_stat_numbers_2 = true && this.isSetStat_numbers_2();
    boolean that_present_stat_numbers_2 = true && that.isSetStat_numbers_2();
    if (this_present_stat_numbers_2 || that_present_stat_numbers_2) {
      if (!(this_present_stat_numbers_2 && that_present_stat_numbers_2))
        return false;
      if (!this.stat_numbers_2.equals(that.stat_numbers_2))
        return false;
    }

    boolean this_present_stat_values_3 = true && this.isSetStat_values_3();
    boolean that_present_stat_values_3 = true && that.isSetStat_values_3();
    if (this_present_stat_values_3 || that_present_stat_values_3) {
      if (!(this_present_stat_values_3 && that_present_stat_values_3))
        return false;
      if (!this.stat_values_3.equals(that.stat_values_3))
        return false;
    }

    boolean this_present_stat_numbers_3 = true && this.isSetStat_numbers_3();
    boolean that_present_stat_numbers_3 = true && that.isSetStat_numbers_3();
    if (this_present_stat_numbers_3 || that_present_stat_numbers_3) {
      if (!(this_present_stat_numbers_3 && that_present_stat_numbers_3))
        return false;
      if (!this.stat_numbers_3.equals(that.stat_numbers_3))
        return false;
    }

    boolean this_present_stat_number_1_type = true;
    boolean that_present_stat_number_1_type = true;
    if (this_present_stat_number_1_type || that_present_stat_number_1_type) {
      if (!(this_present_stat_number_1_type && that_present_stat_number_1_type))
        return false;
      if (this.stat_number_1_type != that.stat_number_1_type)
        return false;
    }

    boolean this_present_stat_number_2_type = true;
    boolean that_present_stat_number_2_type = true;
    if (this_present_stat_number_2_type || that_present_stat_number_2_type) {
      if (!(this_present_stat_number_2_type && that_present_stat_number_2_type))
        return false;
      if (this.stat_number_2_type != that.stat_number_2_type)
        return false;
    }

    boolean this_present_stat_number_3_type = true;
    boolean that_present_stat_number_3_type = true;
    if (this_present_stat_number_3_type || that_present_stat_number_3_type) {
      if (!(this_present_stat_number_3_type && that_present_stat_number_3_type))
        return false;
      if (this.stat_number_3_type != that.stat_number_3_type)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case STAT_TABLE_NAME:
          if (field.type == TType.STRING) {
            this.stat_table_name = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_FIELD_NAME:
          if (field.type == TType.STRING) {
            this.stat_field_name = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NULLFAC:
          if (field.type == TType.DOUBLE) {
            this.stat_nullfac = iprot.readDouble();
            this.__isset.stat_nullfac = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_AVG_FIELD_WIDTH:
          if (field.type == TType.I32) {
            this.stat_avg_field_width = iprot.readI32();
            this.__isset.stat_avg_field_width = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_DISTINCT_VALUES:
          if (field.type == TType.DOUBLE) {
            this.stat_distinct_values = iprot.readDouble();
            this.__isset.stat_distinct_values = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_VALUES_1:
          if (field.type == TType.STRING) {
            this.stat_values_1 = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUMBERS_1:
          if (field.type == TType.STRING) {
            this.stat_numbers_1 = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_VALUES_2:
          if (field.type == TType.STRING) {
            this.stat_values_2 = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUMBERS_2:
          if (field.type == TType.STRING) {
            this.stat_numbers_2 = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_VALUES_3:
          if (field.type == TType.STRING) {
            this.stat_values_3 = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUMBERS_3:
          if (field.type == TType.STRING) {
            this.stat_numbers_3 = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUMBER_1_TYPE:
          if (field.type == TType.I32) {
            this.stat_number_1_type = iprot.readI32();
            this.__isset.stat_number_1_type = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUMBER_2_TYPE:
          if (field.type == TType.I32) {
            this.stat_number_2_type = iprot.readI32();
            this.__isset.stat_number_2_type = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUMBER_3_TYPE:
          if (field.type == TType.I32) {
            this.stat_number_3_type = iprot.readI32();
            this.__isset.stat_number_3_type = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.stat_table_name != null) {
      oprot.writeFieldBegin(STAT_TABLE_NAME_FIELD_DESC);
      oprot.writeString(this.stat_table_name);
      oprot.writeFieldEnd();
    }
    if (this.stat_field_name != null) {
      oprot.writeFieldBegin(STAT_FIELD_NAME_FIELD_DESC);
      oprot.writeString(this.stat_field_name);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(STAT_NULLFAC_FIELD_DESC);
    oprot.writeDouble(this.stat_nullfac);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_AVG_FIELD_WIDTH_FIELD_DESC);
    oprot.writeI32(this.stat_avg_field_width);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_DISTINCT_VALUES_FIELD_DESC);
    oprot.writeDouble(this.stat_distinct_values);
    oprot.writeFieldEnd();
    if (this.stat_values_1 != null) {
      oprot.writeFieldBegin(STAT_VALUES_1_FIELD_DESC);
      oprot.writeString(this.stat_values_1);
      oprot.writeFieldEnd();
    }
    if (this.stat_numbers_1 != null) {
      oprot.writeFieldBegin(STAT_NUMBERS_1_FIELD_DESC);
      oprot.writeString(this.stat_numbers_1);
      oprot.writeFieldEnd();
    }
    if (this.stat_values_2 != null) {
      oprot.writeFieldBegin(STAT_VALUES_2_FIELD_DESC);
      oprot.writeString(this.stat_values_2);
      oprot.writeFieldEnd();
    }
    if (this.stat_numbers_2 != null) {
      oprot.writeFieldBegin(STAT_NUMBERS_2_FIELD_DESC);
      oprot.writeString(this.stat_numbers_2);
      oprot.writeFieldEnd();
    }
    if (this.stat_values_3 != null) {
      oprot.writeFieldBegin(STAT_VALUES_3_FIELD_DESC);
      oprot.writeString(this.stat_values_3);
      oprot.writeFieldEnd();
    }
    if (this.stat_numbers_3 != null) {
      oprot.writeFieldBegin(STAT_NUMBERS_3_FIELD_DESC);
      oprot.writeString(this.stat_numbers_3);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(STAT_NUMBER_1_TYPE_FIELD_DESC);
    oprot.writeI32(this.stat_number_1_type);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_NUMBER_2_TYPE_FIELD_DESC);
    oprot.writeI32(this.stat_number_2_type);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_NUMBER_3_TYPE_FIELD_DESC);
    oprot.writeI32(this.stat_number_3_type);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("tdw_sys_fields_statistics(");
    boolean first = true;

    sb.append("stat_table_name:");
    if (this.stat_table_name == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_table_name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_field_name:");
    if (this.stat_field_name == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_field_name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_nullfac:");
    sb.append(this.stat_nullfac);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_avg_field_width:");
    sb.append(this.stat_avg_field_width);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_distinct_values:");
    sb.append(this.stat_distinct_values);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_values_1:");
    if (this.stat_values_1 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_values_1);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_numbers_1:");
    if (this.stat_numbers_1 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_numbers_1);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_values_2:");
    if (this.stat_values_2 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_values_2);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_numbers_2:");
    if (this.stat_numbers_2 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_numbers_2);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_values_3:");
    if (this.stat_values_3 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_values_3);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_numbers_3:");
    if (this.stat_numbers_3 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_numbers_3);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_number_1_type:");
    sb.append(this.stat_number_1_type);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_number_2_type:");
    sb.append(this.stat_number_2_type);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_number_3_type:");
    sb.append(this.stat_number_3_type);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}

