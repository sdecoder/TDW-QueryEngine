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

public class tdw_sys_table_statistics implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("tdw_sys_table_statistics");
  private static final TField STAT_TABLE_NAME_FIELD_DESC = new TField("stat_table_name", TType.STRING, (short)1);
  private static final TField STAT_NUM_RECORDS_FIELD_DESC = new TField("stat_num_records", TType.I32, (short)2);
  private static final TField STAT_NUM_UNITS_FIELD_DESC = new TField("stat_num_units", TType.I32, (short)3);
  private static final TField STAT_TOTAL_SIZE_FIELD_DESC = new TField("stat_total_size", TType.I32, (short)4);
  private static final TField STAT_NUM_FILES_FIELD_DESC = new TField("stat_num_files", TType.I32, (short)5);
  private static final TField STAT_NUM_BLOCKS_FIELD_DESC = new TField("stat_num_blocks", TType.I32, (short)6);

  private String stat_table_name;
  public static final int STAT_TABLE_NAME = 1;
  private int stat_num_records;
  public static final int STAT_NUM_RECORDS = 2;
  private int stat_num_units;
  public static final int STAT_NUM_UNITS = 3;
  private int stat_total_size;
  public static final int STAT_TOTAL_SIZE = 4;
  private int stat_num_files;
  public static final int STAT_NUM_FILES = 5;
  private int stat_num_blocks;
  public static final int STAT_NUM_BLOCKS = 6;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean stat_num_records = false;
    public boolean stat_num_units = false;
    public boolean stat_total_size = false;
    public boolean stat_num_files = false;
    public boolean stat_num_blocks = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(STAT_TABLE_NAME, new FieldMetaData("stat_table_name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STAT_NUM_RECORDS, new FieldMetaData("stat_num_records", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STAT_NUM_UNITS, new FieldMetaData("stat_num_units", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STAT_TOTAL_SIZE, new FieldMetaData("stat_total_size", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STAT_NUM_FILES, new FieldMetaData("stat_num_files", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STAT_NUM_BLOCKS, new FieldMetaData("stat_num_blocks", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(tdw_sys_table_statistics.class, metaDataMap);
  }

  public tdw_sys_table_statistics() {
  }

  public tdw_sys_table_statistics(
    String stat_table_name,
    int stat_num_records,
    int stat_num_units,
    int stat_total_size,
    int stat_num_files,
    int stat_num_blocks)
  {
    this();
    this.stat_table_name = stat_table_name;
    this.stat_num_records = stat_num_records;
    this.__isset.stat_num_records = true;
    this.stat_num_units = stat_num_units;
    this.__isset.stat_num_units = true;
    this.stat_total_size = stat_total_size;
    this.__isset.stat_total_size = true;
    this.stat_num_files = stat_num_files;
    this.__isset.stat_num_files = true;
    this.stat_num_blocks = stat_num_blocks;
    this.__isset.stat_num_blocks = true;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public tdw_sys_table_statistics(tdw_sys_table_statistics other) {
    if (other.isSetStat_table_name()) {
      this.stat_table_name = other.stat_table_name;
    }
    __isset.stat_num_records = other.__isset.stat_num_records;
    this.stat_num_records = other.stat_num_records;
    __isset.stat_num_units = other.__isset.stat_num_units;
    this.stat_num_units = other.stat_num_units;
    __isset.stat_total_size = other.__isset.stat_total_size;
    this.stat_total_size = other.stat_total_size;
    __isset.stat_num_files = other.__isset.stat_num_files;
    this.stat_num_files = other.stat_num_files;
    __isset.stat_num_blocks = other.__isset.stat_num_blocks;
    this.stat_num_blocks = other.stat_num_blocks;
  }

  @Override
  public tdw_sys_table_statistics clone() {
    return new tdw_sys_table_statistics(this);
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

  public int getStat_num_records() {
    return this.stat_num_records;
  }

  public void setStat_num_records(int stat_num_records) {
    this.stat_num_records = stat_num_records;
    this.__isset.stat_num_records = true;
  }

  public void unsetStat_num_records() {
    this.__isset.stat_num_records = false;
  }

  // Returns true if field stat_num_records is set (has been asigned a value) and false otherwise
  public boolean isSetStat_num_records() {
    return this.__isset.stat_num_records;
  }

  public int getStat_num_units() {
    return this.stat_num_units;
  }

  public void setStat_num_units(int stat_num_units) {
    this.stat_num_units = stat_num_units;
    this.__isset.stat_num_units = true;
  }

  public void unsetStat_num_units() {
    this.__isset.stat_num_units = false;
  }

  // Returns true if field stat_num_units is set (has been asigned a value) and false otherwise
  public boolean isSetStat_num_units() {
    return this.__isset.stat_num_units;
  }

  public int getStat_total_size() {
    return this.stat_total_size;
  }

  public void setStat_total_size(int stat_total_size) {
    this.stat_total_size = stat_total_size;
    this.__isset.stat_total_size = true;
  }

  public void unsetStat_total_size() {
    this.__isset.stat_total_size = false;
  }

  // Returns true if field stat_total_size is set (has been asigned a value) and false otherwise
  public boolean isSetStat_total_size() {
    return this.__isset.stat_total_size;
  }

  public int getStat_num_files() {
    return this.stat_num_files;
  }

  public void setStat_num_files(int stat_num_files) {
    this.stat_num_files = stat_num_files;
    this.__isset.stat_num_files = true;
  }

  public void unsetStat_num_files() {
    this.__isset.stat_num_files = false;
  }

  // Returns true if field stat_num_files is set (has been asigned a value) and false otherwise
  public boolean isSetStat_num_files() {
    return this.__isset.stat_num_files;
  }

  public int getStat_num_blocks() {
    return this.stat_num_blocks;
  }

  public void setStat_num_blocks(int stat_num_blocks) {
    this.stat_num_blocks = stat_num_blocks;
    this.__isset.stat_num_blocks = true;
  }

  public void unsetStat_num_blocks() {
    this.__isset.stat_num_blocks = false;
  }

  // Returns true if field stat_num_blocks is set (has been asigned a value) and false otherwise
  public boolean isSetStat_num_blocks() {
    return this.__isset.stat_num_blocks;
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

    case STAT_NUM_RECORDS:
      if (value == null) {
        unsetStat_num_records();
      } else {
        setStat_num_records((Integer)value);
      }
      break;

    case STAT_NUM_UNITS:
      if (value == null) {
        unsetStat_num_units();
      } else {
        setStat_num_units((Integer)value);
      }
      break;

    case STAT_TOTAL_SIZE:
      if (value == null) {
        unsetStat_total_size();
      } else {
        setStat_total_size((Integer)value);
      }
      break;

    case STAT_NUM_FILES:
      if (value == null) {
        unsetStat_num_files();
      } else {
        setStat_num_files((Integer)value);
      }
      break;

    case STAT_NUM_BLOCKS:
      if (value == null) {
        unsetStat_num_blocks();
      } else {
        setStat_num_blocks((Integer)value);
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

    case STAT_NUM_RECORDS:
      return new Integer(getStat_num_records());

    case STAT_NUM_UNITS:
      return new Integer(getStat_num_units());

    case STAT_TOTAL_SIZE:
      return new Integer(getStat_total_size());

    case STAT_NUM_FILES:
      return new Integer(getStat_num_files());

    case STAT_NUM_BLOCKS:
      return new Integer(getStat_num_blocks());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case STAT_TABLE_NAME:
      return isSetStat_table_name();
    case STAT_NUM_RECORDS:
      return isSetStat_num_records();
    case STAT_NUM_UNITS:
      return isSetStat_num_units();
    case STAT_TOTAL_SIZE:
      return isSetStat_total_size();
    case STAT_NUM_FILES:
      return isSetStat_num_files();
    case STAT_NUM_BLOCKS:
      return isSetStat_num_blocks();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof tdw_sys_table_statistics)
      return this.equals((tdw_sys_table_statistics)that);
    return false;
  }

  public boolean equals(tdw_sys_table_statistics that) {
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

    boolean this_present_stat_num_records = true;
    boolean that_present_stat_num_records = true;
    if (this_present_stat_num_records || that_present_stat_num_records) {
      if (!(this_present_stat_num_records && that_present_stat_num_records))
        return false;
      if (this.stat_num_records != that.stat_num_records)
        return false;
    }

    boolean this_present_stat_num_units = true;
    boolean that_present_stat_num_units = true;
    if (this_present_stat_num_units || that_present_stat_num_units) {
      if (!(this_present_stat_num_units && that_present_stat_num_units))
        return false;
      if (this.stat_num_units != that.stat_num_units)
        return false;
    }

    boolean this_present_stat_total_size = true;
    boolean that_present_stat_total_size = true;
    if (this_present_stat_total_size || that_present_stat_total_size) {
      if (!(this_present_stat_total_size && that_present_stat_total_size))
        return false;
      if (this.stat_total_size != that.stat_total_size)
        return false;
    }

    boolean this_present_stat_num_files = true;
    boolean that_present_stat_num_files = true;
    if (this_present_stat_num_files || that_present_stat_num_files) {
      if (!(this_present_stat_num_files && that_present_stat_num_files))
        return false;
      if (this.stat_num_files != that.stat_num_files)
        return false;
    }

    boolean this_present_stat_num_blocks = true;
    boolean that_present_stat_num_blocks = true;
    if (this_present_stat_num_blocks || that_present_stat_num_blocks) {
      if (!(this_present_stat_num_blocks && that_present_stat_num_blocks))
        return false;
      if (this.stat_num_blocks != that.stat_num_blocks)
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
        case STAT_NUM_RECORDS:
          if (field.type == TType.I32) {
            this.stat_num_records = iprot.readI32();
            this.__isset.stat_num_records = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUM_UNITS:
          if (field.type == TType.I32) {
            this.stat_num_units = iprot.readI32();
            this.__isset.stat_num_units = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_TOTAL_SIZE:
          if (field.type == TType.I32) {
            this.stat_total_size = iprot.readI32();
            this.__isset.stat_total_size = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUM_FILES:
          if (field.type == TType.I32) {
            this.stat_num_files = iprot.readI32();
            this.__isset.stat_num_files = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STAT_NUM_BLOCKS:
          if (field.type == TType.I32) {
            this.stat_num_blocks = iprot.readI32();
            this.__isset.stat_num_blocks = true;
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
    oprot.writeFieldBegin(STAT_NUM_RECORDS_FIELD_DESC);
    oprot.writeI32(this.stat_num_records);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_NUM_UNITS_FIELD_DESC);
    oprot.writeI32(this.stat_num_units);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_TOTAL_SIZE_FIELD_DESC);
    oprot.writeI32(this.stat_total_size);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_NUM_FILES_FIELD_DESC);
    oprot.writeI32(this.stat_num_files);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STAT_NUM_BLOCKS_FIELD_DESC);
    oprot.writeI32(this.stat_num_blocks);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("tdw_sys_table_statistics(");
    boolean first = true;

    sb.append("stat_table_name:");
    if (this.stat_table_name == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_table_name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_num_records:");
    sb.append(this.stat_num_records);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_num_units:");
    sb.append(this.stat_num_units);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_total_size:");
    sb.append(this.stat_total_size);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_num_files:");
    sb.append(this.stat_num_files);
    first = false;
    if (!first) sb.append(", ");
    sb.append("stat_num_blocks:");
    sb.append(this.stat_num_blocks);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}
