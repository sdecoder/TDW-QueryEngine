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

public class Partition implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("Partition");
  private static final TField DB_NAME_FIELD_DESC = new TField("dbName", TType.STRING, (short)1);
  private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)2);
  private static final TField LEVEL_FIELD_DESC = new TField("level", TType.I32, (short)3);
  private static final TField PAR_TYPE_FIELD_DESC = new TField("parType", TType.STRING, (short)4);
  private static final TField PAR_KEY_FIELD_DESC = new TField("parKey", TType.STRUCT, (short)5);
  private static final TField PAR_SPACES_FIELD_DESC = new TField("parSpaces", TType.MAP, (short)6);

  private String dbName;
  public static final int DBNAME = 1;
  private String tableName;
  public static final int TABLENAME = 2;
  private int level;
  public static final int LEVEL = 3;
  private String parType;
  public static final int PARTYPE = 4;
  private FieldSchema parKey;
  public static final int PARKEY = 5;
  private Map<String,List<String>> parSpaces;
  public static final int PARSPACES = 6;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean level = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(DBNAME, new FieldMetaData("dbName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(LEVEL, new FieldMetaData("level", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(PARTYPE, new FieldMetaData("parType", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(PARKEY, new FieldMetaData("parKey", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, FieldSchema.class)));
    put(PARSPACES, new FieldMetaData("parSpaces", TFieldRequirementType.DEFAULT, 
        new MapMetaData(TType.MAP, 
            new FieldValueMetaData(TType.STRING), 
            new ListMetaData(TType.LIST, 
                new FieldValueMetaData(TType.STRING)))));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(Partition.class, metaDataMap);
  }

  public Partition() {
  }

  public Partition(
    String dbName,
    String tableName,
    int level,
    String parType,
    FieldSchema parKey,
    Map<String,List<String>> parSpaces)
  {
    this();
    this.dbName = dbName;
    this.tableName = tableName;
    this.level = level;
    this.__isset.level = true;
    this.parType = parType;
    this.parKey = parKey;
    this.parSpaces = parSpaces;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Partition(Partition other) {
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    __isset.level = other.__isset.level;
    this.level = other.level;
    if (other.isSetParType()) {
      this.parType = other.parType;
    }
    if (other.isSetParKey()) {
      this.parKey = new FieldSchema(other.parKey);
    }
    if (other.isSetParSpaces()) {
      Map<String,List<String>> __this__parSpaces = new HashMap<String,List<String>>();
      for (Map.Entry<String, List<String>> other_element : other.parSpaces.entrySet()) {

        String other_element_key = other_element.getKey();
        List<String> other_element_value = other_element.getValue();

        String __this__parSpaces_copy_key = other_element_key;

        List<String> __this__parSpaces_copy_value = new ArrayList<String>();
        for (String other_element_value_element : other_element_value) {
          __this__parSpaces_copy_value.add(other_element_value_element);
        }

        __this__parSpaces.put(__this__parSpaces_copy_key, __this__parSpaces_copy_value);
      }
      this.parSpaces = __this__parSpaces;
    }
  }

  @Override
  public Partition clone() {
    return new Partition(this);
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

  public int getLevel() {
    return this.level;
  }

  public void setLevel(int level) {
    this.level = level;
    this.__isset.level = true;
  }

  public void unsetLevel() {
    this.__isset.level = false;
  }

  // Returns true if field level is set (has been asigned a value) and false otherwise
  public boolean isSetLevel() {
    return this.__isset.level;
  }

  public String getParType() {
    return this.parType;
  }

  public void setParType(String parType) {
    this.parType = parType;
  }

  public void unsetParType() {
    this.parType = null;
  }

  // Returns true if field parType is set (has been asigned a value) and false otherwise
  public boolean isSetParType() {
    return this.parType != null;
  }

  public FieldSchema getParKey() {
    return this.parKey;
  }

  public void setParKey(FieldSchema parKey) {
    this.parKey = parKey;
  }

  public void unsetParKey() {
    this.parKey = null;
  }

  // Returns true if field parKey is set (has been asigned a value) and false otherwise
  public boolean isSetParKey() {
    return this.parKey != null;
  }

  public int getParSpacesSize() {
    return (this.parSpaces == null) ? 0 : this.parSpaces.size();
  }

  public void putToParSpaces(String key, List<String> val) {
    if (this.parSpaces == null) {
      this.parSpaces = new HashMap<String,List<String>>();
    }
    this.parSpaces.put(key, val);
  }

  public Map<String,List<String>> getParSpaces() {
    return this.parSpaces;
  }

  public void setParSpaces(Map<String,List<String>> parSpaces) {
    this.parSpaces = parSpaces;
  }

  public void unsetParSpaces() {
    this.parSpaces = null;
  }

  // Returns true if field parSpaces is set (has been asigned a value) and false otherwise
  public boolean isSetParSpaces() {
    return this.parSpaces != null;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case DBNAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    case TABLENAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String)value);
      }
      break;

    case LEVEL:
      if (value == null) {
        unsetLevel();
      } else {
        setLevel((Integer)value);
      }
      break;

    case PARTYPE:
      if (value == null) {
        unsetParType();
      } else {
        setParType((String)value);
      }
      break;

    case PARKEY:
      if (value == null) {
        unsetParKey();
      } else {
        setParKey((FieldSchema)value);
      }
      break;

    case PARSPACES:
      if (value == null) {
        unsetParSpaces();
      } else {
        setParSpaces((Map<String,List<String>>)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case DBNAME:
      return getDbName();

    case TABLENAME:
      return getTableName();

    case LEVEL:
      return new Integer(getLevel());

    case PARTYPE:
      return getParType();

    case PARKEY:
      return getParKey();

    case PARSPACES:
      return getParSpaces();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case DBNAME:
      return isSetDbName();
    case TABLENAME:
      return isSetTableName();
    case LEVEL:
      return isSetLevel();
    case PARTYPE:
      return isSetParType();
    case PARKEY:
      return isSetParKey();
    case PARSPACES:
      return isSetParSpaces();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Partition)
      return this.equals((Partition)that);
    return false;
  }

  public boolean equals(Partition that) {
    if (that == null)
      return false;

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_level = true;
    boolean that_present_level = true;
    if (this_present_level || that_present_level) {
      if (!(this_present_level && that_present_level))
        return false;
      if (this.level != that.level)
        return false;
    }

    boolean this_present_parType = true && this.isSetParType();
    boolean that_present_parType = true && that.isSetParType();
    if (this_present_parType || that_present_parType) {
      if (!(this_present_parType && that_present_parType))
        return false;
      if (!this.parType.equals(that.parType))
        return false;
    }

    boolean this_present_parKey = true && this.isSetParKey();
    boolean that_present_parKey = true && that.isSetParKey();
    if (this_present_parKey || that_present_parKey) {
      if (!(this_present_parKey && that_present_parKey))
        return false;
      if (!this.parKey.equals(that.parKey))
        return false;
    }

    boolean this_present_parSpaces = true && this.isSetParSpaces();
    boolean that_present_parSpaces = true && that.isSetParSpaces();
    if (this_present_parSpaces || that_present_parSpaces) {
      if (!(this_present_parSpaces && that_present_parSpaces))
        return false;
      if (!this.parSpaces.equals(that.parSpaces))
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
        case DBNAME:
          if (field.type == TType.STRING) {
            this.dbName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case TABLENAME:
          if (field.type == TType.STRING) {
            this.tableName = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LEVEL:
          if (field.type == TType.I32) {
            this.level = iprot.readI32();
            this.__isset.level = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PARTYPE:
          if (field.type == TType.STRING) {
            this.parType = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PARKEY:
          if (field.type == TType.STRUCT) {
            this.parKey = new FieldSchema();
            this.parKey.read(iprot);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PARSPACES:
          if (field.type == TType.MAP) {
            {
              TMap _map26 = iprot.readMapBegin();
              this.parSpaces = new HashMap<String,List<String>>(2*_map26.size);
              for (int _i27 = 0; _i27 < _map26.size; ++_i27)
              {
                String _key28;
                List<String> _val29;
                _key28 = iprot.readString();
                {
                  TList _list30 = iprot.readListBegin();
                  _val29 = new ArrayList<String>(_list30.size);
                  for (int _i31 = 0; _i31 < _list30.size; ++_i31)
                  {
                    String _elem32;
                    _elem32 = iprot.readString();
                    _val29.add(_elem32);
                  }
                  iprot.readListEnd();
                }
                this.parSpaces.put(_key28, _val29);
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
    if (this.dbName != null) {
      oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
      oprot.writeString(this.dbName);
      oprot.writeFieldEnd();
    }
    if (this.tableName != null) {
      oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
      oprot.writeString(this.tableName);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(LEVEL_FIELD_DESC);
    oprot.writeI32(this.level);
    oprot.writeFieldEnd();
    if (this.parType != null) {
      oprot.writeFieldBegin(PAR_TYPE_FIELD_DESC);
      oprot.writeString(this.parType);
      oprot.writeFieldEnd();
    }
    if (this.parKey != null) {
      oprot.writeFieldBegin(PAR_KEY_FIELD_DESC);
      this.parKey.write(oprot);
      oprot.writeFieldEnd();
    }
    if (this.parSpaces != null) {
      oprot.writeFieldBegin(PAR_SPACES_FIELD_DESC);
      {
        oprot.writeMapBegin(new TMap(TType.STRING, TType.LIST, this.parSpaces.size()));
        for (Map.Entry<String, List<String>> _iter33 : this.parSpaces.entrySet())        {
          oprot.writeString(_iter33.getKey());
          {
            oprot.writeListBegin(new TList(TType.STRING, _iter33.getValue().size()));
            for (String _iter34 : _iter33.getValue())            {
              oprot.writeString(_iter34);
            }
            oprot.writeListEnd();
          }
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
    StringBuilder sb = new StringBuilder("Partition(");
    boolean first = true;

    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tableName:");
    if (this.tableName == null) {
      sb.append("null");
    } else {
      sb.append(this.tableName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("level:");
    sb.append(this.level);
    first = false;
    if (!first) sb.append(", ");
    sb.append("parType:");
    if (this.parType == null) {
      sb.append("null");
    } else {
      sb.append(this.parType);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("parKey:");
    if (this.parKey == null) {
      sb.append("null");
    } else {
      sb.append(this.parKey);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("parSpaces:");
    if (this.parSpaces == null) {
      sb.append("null");
    } else {
      sb.append(this.parSpaces);
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

