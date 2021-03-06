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

public class Table implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("Table");
  private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
  private static final TField DB_NAME_FIELD_DESC = new TField("dbName", TType.STRING, (short)2);
  private static final TField OWNER_FIELD_DESC = new TField("owner", TType.STRING, (short)3);
  private static final TField CREATE_TIME_FIELD_DESC = new TField("createTime", TType.I32, (short)4);
  private static final TField LAST_ACCESS_TIME_FIELD_DESC = new TField("lastAccessTime", TType.I32, (short)5);
  private static final TField RETENTION_FIELD_DESC = new TField("retention", TType.I32, (short)6);
  private static final TField SD_FIELD_DESC = new TField("sd", TType.STRUCT, (short)7);
  private static final TField PRI_PARTITION_FIELD_DESC = new TField("priPartition", TType.STRUCT, (short)8);
  private static final TField SUB_PARTITION_FIELD_DESC = new TField("subPartition", TType.STRUCT, (short)9);
  private static final TField PARAMETERS_FIELD_DESC = new TField("parameters", TType.MAP, (short)10);

  private String tableName;
  public static final int TABLENAME = 1;
  private String dbName;
  public static final int DBNAME = 2;
  private String owner;
  public static final int OWNER = 3;
  private int createTime;
  public static final int CREATETIME = 4;
  private int lastAccessTime;
  public static final int LASTACCESSTIME = 5;
  private int retention;
  public static final int RETENTION = 6;
  private StorageDescriptor sd;
  public static final int SD = 7;
  private Partition priPartition;
  public static final int PRIPARTITION = 8;
  private Partition subPartition;
  public static final int SUBPARTITION = 9;
  private Map<String,String> parameters;
  public static final int PARAMETERS = 10;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean createTime = false;
    public boolean lastAccessTime = false;
    public boolean retention = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(DBNAME, new FieldMetaData("dbName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(OWNER, new FieldMetaData("owner", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(CREATETIME, new FieldMetaData("createTime", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(LASTACCESSTIME, new FieldMetaData("lastAccessTime", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(RETENTION, new FieldMetaData("retention", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(SD, new FieldMetaData("sd", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, StorageDescriptor.class)));
    put(PRIPARTITION, new FieldMetaData("priPartition", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, Partition.class)));
    put(SUBPARTITION, new FieldMetaData("subPartition", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, Partition.class)));
    put(PARAMETERS, new FieldMetaData("parameters", TFieldRequirementType.DEFAULT, 
        new MapMetaData(TType.MAP, 
            new FieldValueMetaData(TType.STRING), 
            new FieldValueMetaData(TType.STRING))));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(Table.class, metaDataMap);
  }

  public Table() {
  }

  public Table(
    String tableName,
    String dbName,
    String owner,
    int createTime,
    int lastAccessTime,
    int retention,
    StorageDescriptor sd,
    Partition priPartition,
    Partition subPartition,
    Map<String,String> parameters)
  {
    this();
    this.tableName = tableName;
    this.dbName = dbName;
    this.owner = owner;
    this.createTime = createTime;
    this.__isset.createTime = true;
    this.lastAccessTime = lastAccessTime;
    this.__isset.lastAccessTime = true;
    this.retention = retention;
    this.__isset.retention = true;
    this.sd = sd;
    this.priPartition = priPartition;
    this.subPartition = subPartition;
    this.parameters = parameters;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Table(Table other) {
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetOwner()) {
      this.owner = other.owner;
    }
    __isset.createTime = other.__isset.createTime;
    this.createTime = other.createTime;
    __isset.lastAccessTime = other.__isset.lastAccessTime;
    this.lastAccessTime = other.lastAccessTime;
    __isset.retention = other.__isset.retention;
    this.retention = other.retention;
    if (other.isSetSd()) {
      this.sd = new StorageDescriptor(other.sd);
    }
    if (other.isSetPriPartition()) {
      this.priPartition = new Partition(other.priPartition);
    }
    if (other.isSetSubPartition()) {
      this.subPartition = new Partition(other.subPartition);
    }
    if (other.isSetParameters()) {
      Map<String,String> __this__parameters = new HashMap<String,String>();
      for (Map.Entry<String, String> other_element : other.parameters.entrySet()) {

        String other_element_key = other_element.getKey();
        String other_element_value = other_element.getValue();

        String __this__parameters_copy_key = other_element_key;

        String __this__parameters_copy_value = other_element_value;

        __this__parameters.put(__this__parameters_copy_key, __this__parameters_copy_value);
      }
      this.parameters = __this__parameters;
    }
  }

  @Override
  public Table clone() {
    return new Table(this);
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void unsetTableName() {
    this.tableName = null;
  }

  // Returns true if field tableName is set (has been asigned a value) and false otherwise
  public boolean isSetTableName() {
    return this.tableName != null;
  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  // Returns true if field dbName is set (has been asigned a value) and false otherwise
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public String getOwner() {
    return this.owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public void unsetOwner() {
    this.owner = null;
  }

  // Returns true if field owner is set (has been asigned a value) and false otherwise
  public boolean isSetOwner() {
    return this.owner != null;
  }

  public int getCreateTime() {
    return this.createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
    this.__isset.createTime = true;
  }

  public void unsetCreateTime() {
    this.__isset.createTime = false;
  }

  // Returns true if field createTime is set (has been asigned a value) and false otherwise
  public boolean isSetCreateTime() {
    return this.__isset.createTime;
  }

  public int getLastAccessTime() {
    return this.lastAccessTime;
  }

  public void setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
    this.__isset.lastAccessTime = true;
  }

  public void unsetLastAccessTime() {
    this.__isset.lastAccessTime = false;
  }

  // Returns true if field lastAccessTime is set (has been asigned a value) and false otherwise
  public boolean isSetLastAccessTime() {
    return this.__isset.lastAccessTime;
  }

  public int getRetention() {
    return this.retention;
  }

  public void setRetention(int retention) {
    this.retention = retention;
    this.__isset.retention = true;
  }

  public void unsetRetention() {
    this.__isset.retention = false;
  }

  // Returns true if field retention is set (has been asigned a value) and false otherwise
  public boolean isSetRetention() {
    return this.__isset.retention;
  }

  public StorageDescriptor getSd() {
    return this.sd;
  }

  public void setSd(StorageDescriptor sd) {
    this.sd = sd;
  }

  public void unsetSd() {
    this.sd = null;
  }

  // Returns true if field sd is set (has been asigned a value) and false otherwise
  public boolean isSetSd() {
    return this.sd != null;
  }

  public Partition getPriPartition() {
    return this.priPartition;
  }

  public void setPriPartition(Partition priPartition) {
    this.priPartition = priPartition;
  }

  public void unsetPriPartition() {
    this.priPartition = null;
  }

  // Returns true if field priPartition is set (has been asigned a value) and false otherwise
  public boolean isSetPriPartition() {
    return this.priPartition != null;
  }

  public Partition getSubPartition() {
    return this.subPartition;
  }

  public void setSubPartition(Partition subPartition) {
    this.subPartition = subPartition;
  }

  public void unsetSubPartition() {
    this.subPartition = null;
  }

  // Returns true if field subPartition is set (has been asigned a value) and false otherwise
  public boolean isSetSubPartition() {
    return this.subPartition != null;
  }

  public int getParametersSize() {
    return (this.parameters == null) ? 0 : this.parameters.size();
  }

  public void putToParameters(String key, String val) {
    if (this.parameters == null) {
      this.parameters = new HashMap<String,String>();
    }
    this.parameters.put(key, val);
  }

  public Map<String,String> getParameters() {
    return this.parameters;
  }

  public void setParameters(Map<String,String> parameters) {
    this.parameters = parameters;
  }

  public void unsetParameters() {
    this.parameters = null;
  }

  // Returns true if field parameters is set (has been asigned a value) and false otherwise
  public boolean isSetParameters() {
    return this.parameters != null;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case TABLENAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String)value);
      }
      break;

    case DBNAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    case OWNER:
      if (value == null) {
        unsetOwner();
      } else {
        setOwner((String)value);
      }
      break;

    case CREATETIME:
      if (value == null) {
        unsetCreateTime();
      } else {
        setCreateTime((Integer)value);
      }
      break;

    case LASTACCESSTIME:
      if (value == null) {
        unsetLastAccessTime();
      } else {
        setLastAccessTime((Integer)value);
      }
      break;

    case RETENTION:
      if (value == null) {
        unsetRetention();
      } else {
        setRetention((Integer)value);
      }
      break;

    case SD:
      if (value == null) {
        unsetSd();
      } else {
        setSd((StorageDescriptor)value);
      }
      break;

    case PRIPARTITION:
      if (value == null) {
        unsetPriPartition();
      } else {
        setPriPartition((Partition)value);
      }
      break;

    case SUBPARTITION:
      if (value == null) {
        unsetSubPartition();
      } else {
        setSubPartition((Partition)value);
      }
      break;

    case PARAMETERS:
      if (value == null) {
        unsetParameters();
      } else {
        setParameters((Map<String,String>)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case TABLENAME:
      return getTableName();

    case DBNAME:
      return getDbName();

    case OWNER:
      return getOwner();

    case CREATETIME:
      return new Integer(getCreateTime());

    case LASTACCESSTIME:
      return new Integer(getLastAccessTime());

    case RETENTION:
      return new Integer(getRetention());

    case SD:
      return getSd();

    case PRIPARTITION:
      return getPriPartition();

    case SUBPARTITION:
      return getSubPartition();

    case PARAMETERS:
      return getParameters();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case TABLENAME:
      return isSetTableName();
    case DBNAME:
      return isSetDbName();
    case OWNER:
      return isSetOwner();
    case CREATETIME:
      return isSetCreateTime();
    case LASTACCESSTIME:
      return isSetLastAccessTime();
    case RETENTION:
      return isSetRetention();
    case SD:
      return isSetSd();
    case PRIPARTITION:
      return isSetPriPartition();
    case SUBPARTITION:
      return isSetSubPartition();
    case PARAMETERS:
      return isSetParameters();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Table)
      return this.equals((Table)that);
    return false;
  }

  public boolean equals(Table that) {
    if (that == null)
      return false;

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_owner = true && this.isSetOwner();
    boolean that_present_owner = true && that.isSetOwner();
    if (this_present_owner || that_present_owner) {
      if (!(this_present_owner && that_present_owner))
        return false;
      if (!this.owner.equals(that.owner))
        return false;
    }

    boolean this_present_createTime = true;
    boolean that_present_createTime = true;
    if (this_present_createTime || that_present_createTime) {
      if (!(this_present_createTime && that_present_createTime))
        return false;
      if (this.createTime != that.createTime)
        return false;
    }

    boolean this_present_lastAccessTime = true;
    boolean that_present_lastAccessTime = true;
    if (this_present_lastAccessTime || that_present_lastAccessTime) {
      if (!(this_present_lastAccessTime && that_present_lastAccessTime))
        return false;
      if (this.lastAccessTime != that.lastAccessTime)
        return false;
    }

    boolean this_present_retention = true;
    boolean that_present_retention = true;
    if (this_present_retention || that_present_retention) {
      if (!(this_present_retention && that_present_retention))
        return false;
      if (this.retention != that.retention)
        return false;
    }

    boolean this_present_sd = true && this.isSetSd();
    boolean that_present_sd = true && that.isSetSd();
    if (this_present_sd || that_present_sd) {
      if (!(this_present_sd && that_present_sd))
        return false;
      if (!this.sd.equals(that.sd))
        return false;
    }

    boolean this_present_priPartition = true && this.isSetPriPartition();
    boolean that_present_priPartition = true && that.isSetPriPartition();
    if (this_present_priPartition || that_present_priPartition) {
      if (!(this_present_priPartition && that_present_priPartition))
        return false;
      if (!this.priPartition.equals(that.priPartition))
        return false;
    }

    boolean this_present_subPartition = true && this.isSetSubPartition();
    boolean that_present_subPartition = true && that.isSetSubPartition();
    if (this_present_subPartition || that_present_subPartition) {
      if (!(this_present_subPartition && that_present_subPartition))
        return false;
      if (!this.subPartition.equals(that.subPartition))
        return false;
    }

    boolean this_present_parameters = true && this.isSetParameters();
    boolean that_present_parameters = true && that.isSetParameters();
    if (this_present_parameters || that_present_parameters) {
      if (!(this_present_parameters && that_present_parameters))
        return false;
      if (!this.parameters.equals(that.parameters))
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
        case TABLENAME:
          if (field.type == TType.STRING) {
            this.tableName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case DBNAME:
          if (field.type == TType.STRING) {
            this.dbName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case OWNER:
          if (field.type == TType.STRING) {
            this.owner = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case CREATETIME:
          if (field.type == TType.I32) {
            this.createTime = iprot.readI32();
            this.__isset.createTime = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LASTACCESSTIME:
          if (field.type == TType.I32) {
            this.lastAccessTime = iprot.readI32();
            this.__isset.lastAccessTime = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case RETENTION:
          if (field.type == TType.I32) {
            this.retention = iprot.readI32();
            this.__isset.retention = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case SD:
          if (field.type == TType.STRUCT) {
            this.sd = new StorageDescriptor();
            this.sd.read(iprot);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PRIPARTITION:
          if (field.type == TType.STRUCT) {
            this.priPartition = new Partition();
            this.priPartition.read(iprot);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case SUBPARTITION:
          if (field.type == TType.STRUCT) {
            this.subPartition = new Partition();
            this.subPartition.read(iprot);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PARAMETERS:
          if (field.type == TType.MAP) {
            {
              TMap _map35 = iprot.readMapBegin();
              this.parameters = new HashMap<String,String>(2*_map35.size);
              for (int _i36 = 0; _i36 < _map35.size; ++_i36)
              {
                String _key37;
                String _val38;
                _key37 = iprot.readString();
                _val38 = iprot.readString();
                this.parameters.put(_key37, _val38);
              }
              iprot.readMapEnd();
            }
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
    if (this.tableName != null) {
      oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
      oprot.writeString(this.tableName);
      oprot.writeFieldEnd();
    }
    if (this.dbName != null) {
      oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
      oprot.writeString(this.dbName);
      oprot.writeFieldEnd();
    }
    if (this.owner != null) {
      oprot.writeFieldBegin(OWNER_FIELD_DESC);
      oprot.writeString(this.owner);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(CREATE_TIME_FIELD_DESC);
    oprot.writeI32(this.createTime);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(LAST_ACCESS_TIME_FIELD_DESC);
    oprot.writeI32(this.lastAccessTime);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(RETENTION_FIELD_DESC);
    oprot.writeI32(this.retention);
    oprot.writeFieldEnd();
    if (this.sd != null) {
      oprot.writeFieldBegin(SD_FIELD_DESC);
      this.sd.write(oprot);
      oprot.writeFieldEnd();
    }
    if (this.priPartition != null) {
      oprot.writeFieldBegin(PRI_PARTITION_FIELD_DESC);
      this.priPartition.write(oprot);
      oprot.writeFieldEnd();
    }
    if (this.subPartition != null) {
      oprot.writeFieldBegin(SUB_PARTITION_FIELD_DESC);
      this.subPartition.write(oprot);
      oprot.writeFieldEnd();
    }
    if (this.parameters != null) {
      oprot.writeFieldBegin(PARAMETERS_FIELD_DESC);
      {
        oprot.writeMapBegin(new TMap(TType.STRING, TType.STRING, this.parameters.size()));
        for (Map.Entry<String, String> _iter39 : this.parameters.entrySet())        {
          oprot.writeString(_iter39.getKey());
          oprot.writeString(_iter39.getValue());
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Table(");
    boolean first = true;

    sb.append("tableName:");
    if (this.tableName == null) {
      sb.append("null");
    } else {
      sb.append(this.tableName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("owner:");
    if (this.owner == null) {
      sb.append("null");
    } else {
      sb.append(this.owner);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("createTime:");
    sb.append(this.createTime);
    first = false;
    if (!first) sb.append(", ");
    sb.append("lastAccessTime:");
    sb.append(this.lastAccessTime);
    first = false;
    if (!first) sb.append(", ");
    sb.append("retention:");
    sb.append(this.retention);
    first = false;
    if (!first) sb.append(", ");
    sb.append("sd:");
    if (this.sd == null) {
      sb.append("null");
    } else {
      sb.append(this.sd);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("priPartition:");
    if (this.priPartition == null) {
      sb.append("null");
    } else {
      sb.append(this.priPartition);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("subPartition:");
    if (this.subPartition == null) {
      sb.append("null");
    } else {
      sb.append(this.subPartition);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("parameters:");
    if (this.parameters == null) {
      sb.append("null");
    } else {
      sb.append(this.parameters);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}

