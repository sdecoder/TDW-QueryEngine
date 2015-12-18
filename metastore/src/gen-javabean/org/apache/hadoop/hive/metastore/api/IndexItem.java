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

public class IndexItem implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("IndexItem");
  private static final TField DB_FIELD_DESC = new TField("db", TType.STRING, (short)1);
  private static final TField TBL_FIELD_DESC = new TField("tbl", TType.STRING, (short)2);
  private static final TField NAME_FIELD_DESC = new TField("name", TType.STRING, (short)3);
  private static final TField FIELD_LIST_FIELD_DESC = new TField("fieldList", TType.STRING, (short)4);
  private static final TField LOCATION_FIELD_DESC = new TField("location", TType.STRING, (short)5);
  private static final TField INDEX_PATH_FIELD_DESC = new TField("indexPath", TType.STRING, (short)6);
  private static final TField PART_PATH_FIELD_DESC = new TField("partPath", TType.SET, (short)7);
  private static final TField TYPE_FIELD_DESC = new TField("type", TType.I32, (short)8);
  private static final TField STATUS_FIELD_DESC = new TField("status", TType.I32, (short)9);

  private String db;
  public static final int DB = 1;
  private String tbl;
  public static final int TBL = 2;
  private String name;
  public static final int NAME = 3;
  private String fieldList;
  public static final int FIELDLIST = 4;
  private String location;
  public static final int LOCATION = 5;
  private String indexPath;
  public static final int INDEXPATH = 6;
  private Set<String> partPath;
  public static final int PARTPATH = 7;
  private int type;
  public static final int TYPE = 8;
  private int status;
  public static final int STATUS = 9;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean type = false;
    public boolean status = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(DB, new FieldMetaData("db", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(TBL, new FieldMetaData("tbl", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(NAME, new FieldMetaData("name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(FIELDLIST, new FieldMetaData("fieldList", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(LOCATION, new FieldMetaData("location", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(INDEXPATH, new FieldMetaData("indexPath", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(PARTPATH, new FieldMetaData("partPath", TFieldRequirementType.DEFAULT, 
        new SetMetaData(TType.SET, 
            new FieldValueMetaData(TType.STRING))));
    put(TYPE, new FieldMetaData("type", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STATUS, new FieldMetaData("status", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(IndexItem.class, metaDataMap);
  }

  public IndexItem() {
  }

  public IndexItem(
    String db,
    String tbl,
    String name,
    String fieldList,
    String location,
    String indexPath,
    Set<String> partPath,
    int type,
    int status)
  {
    this();
    this.db = db;
    this.tbl = tbl;
    this.name = name;
    this.fieldList = fieldList;
    this.location = location;
    this.indexPath = indexPath;
    this.partPath = partPath;
    this.type = type;
    this.__isset.type = true;
    this.status = status;
    this.__isset.status = true;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public IndexItem(IndexItem other) {
    if (other.isSetDb()) {
      this.db = other.db;
    }
    if (other.isSetTbl()) {
      this.tbl = other.tbl;
    }
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetFieldList()) {
      this.fieldList = other.fieldList;
    }
    if (other.isSetLocation()) {
      this.location = other.location;
    }
    if (other.isSetIndexPath()) {
      this.indexPath = other.indexPath;
    }
    if (other.isSetPartPath()) {
      Set<String> __this__partPath = new HashSet<String>();
      for (String other_element : other.partPath) {
        __this__partPath.add(other_element);
      }
      this.partPath = __this__partPath;
    }
    __isset.type = other.__isset.type;
    this.type = other.type;
    __isset.status = other.__isset.status;
    this.status = other.status;
  }

  @Override
  public IndexItem clone() {
    return new IndexItem(this);
  }

  public String getDb() {
    return this.db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public void unsetDb() {
    this.db = null;
  }

  // Returns true if field db is set (has been asigned a value) and false otherwise
  public boolean isSetDb() {
    return this.db != null;
  }

  public String getTbl() {
    return this.tbl;
  }

  public void setTbl(String tbl) {
    this.tbl = tbl;
  }

  public void unsetTbl() {
    this.tbl = null;
  }

  // Returns true if field tbl is set (has been asigned a value) and false otherwise
  public boolean isSetTbl() {
    return this.tbl != null;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void unsetName() {
    this.name = null;
  }

  // Returns true if field name is set (has been asigned a value) and false otherwise
  public boolean isSetName() {
    return this.name != null;
  }

  public String getFieldList() {
    return this.fieldList;
  }

  public void setFieldList(String fieldList) {
    this.fieldList = fieldList;
  }

  public void unsetFieldList() {
    this.fieldList = null;
  }

  // Returns true if field fieldList is set (has been asigned a value) and false otherwise
  public boolean isSetFieldList() {
    return this.fieldList != null;
  }

  public String getLocation() {
    return this.location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public void unsetLocation() {
    this.location = null;
  }

  // Returns true if field location is set (has been asigned a value) and false otherwise
  public boolean isSetLocation() {
    return this.location != null;
  }

  public String getIndexPath() {
    return this.indexPath;
  }

  public void setIndexPath(String indexPath) {
    this.indexPath = indexPath;
  }

  public void unsetIndexPath() {
    this.indexPath = null;
  }

  // Returns true if field indexPath is set (has been asigned a value) and false otherwise
  public boolean isSetIndexPath() {
    return this.indexPath != null;
  }

  public int getPartPathSize() {
    return (this.partPath == null) ? 0 : this.partPath.size();
  }

  public java.util.Iterator<String> getPartPathIterator() {
    return (this.partPath == null) ? null : this.partPath.iterator();
  }

  public void addToPartPath(String elem) {
    if (this.partPath == null) {
      this.partPath = new HashSet<String>();
    }
    this.partPath.add(elem);
  }

  public Set<String> getPartPath() {
    return this.partPath;
  }

  public void setPartPath(Set<String> partPath) {
    this.partPath = partPath;
  }

  public void unsetPartPath() {
    this.partPath = null;
  }

  // Returns true if field partPath is set (has been asigned a value) and false otherwise
  public boolean isSetPartPath() {
    return this.partPath != null;
  }

  public int getType() {
    return this.type;
  }

  public void setType(int type) {
    this.type = type;
    this.__isset.type = true;
  }

  public void unsetType() {
    this.__isset.type = false;
  }

  // Returns true if field type is set (has been asigned a value) and false otherwise
  public boolean isSetType() {
    return this.__isset.type;
  }

  public int getStatus() {
    return this.status;
  }

  public void setStatus(int status) {
    this.status = status;
    this.__isset.status = true;
  }

  public void unsetStatus() {
    this.__isset.status = false;
  }

  // Returns true if field status is set (has been asigned a value) and false otherwise
  public boolean isSetStatus() {
    return this.__isset.status;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case DB:
      if (value == null) {
        unsetDb();
      } else {
        setDb((String)value);
      }
      break;

    case TBL:
      if (value == null) {
        unsetTbl();
      } else {
        setTbl((String)value);
      }
      break;

    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((String)value);
      }
      break;

    case FIELDLIST:
      if (value == null) {
        unsetFieldList();
      } else {
        setFieldList((String)value);
      }
      break;

    case LOCATION:
      if (value == null) {
        unsetLocation();
      } else {
        setLocation((String)value);
      }
      break;

    case INDEXPATH:
      if (value == null) {
        unsetIndexPath();
      } else {
        setIndexPath((String)value);
      }
      break;

    case PARTPATH:
      if (value == null) {
        unsetPartPath();
      } else {
        setPartPath((Set<String>)value);
      }
      break;

    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((Integer)value);
      }
      break;

    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((Integer)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case DB:
      return getDb();

    case TBL:
      return getTbl();

    case NAME:
      return getName();

    case FIELDLIST:
      return getFieldList();

    case LOCATION:
      return getLocation();

    case INDEXPATH:
      return getIndexPath();

    case PARTPATH:
      return getPartPath();

    case TYPE:
      return new Integer(getType());

    case STATUS:
      return new Integer(getStatus());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case DB:
      return isSetDb();
    case TBL:
      return isSetTbl();
    case NAME:
      return isSetName();
    case FIELDLIST:
      return isSetFieldList();
    case LOCATION:
      return isSetLocation();
    case INDEXPATH:
      return isSetIndexPath();
    case PARTPATH:
      return isSetPartPath();
    case TYPE:
      return isSetType();
    case STATUS:
      return isSetStatus();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof IndexItem)
      return this.equals((IndexItem)that);
    return false;
  }

  public boolean equals(IndexItem that) {
    if (that == null)
      return false;

    boolean this_present_db = true && this.isSetDb();
    boolean that_present_db = true && that.isSetDb();
    if (this_present_db || that_present_db) {
      if (!(this_present_db && that_present_db))
        return false;
      if (!this.db.equals(that.db))
        return false;
    }

    boolean this_present_tbl = true && this.isSetTbl();
    boolean that_present_tbl = true && that.isSetTbl();
    if (this_present_tbl || that_present_tbl) {
      if (!(this_present_tbl && that_present_tbl))
        return false;
      if (!this.tbl.equals(that.tbl))
        return false;
    }

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_fieldList = true && this.isSetFieldList();
    boolean that_present_fieldList = true && that.isSetFieldList();
    if (this_present_fieldList || that_present_fieldList) {
      if (!(this_present_fieldList && that_present_fieldList))
        return false;
      if (!this.fieldList.equals(that.fieldList))
        return false;
    }

    boolean this_present_location = true && this.isSetLocation();
    boolean that_present_location = true && that.isSetLocation();
    if (this_present_location || that_present_location) {
      if (!(this_present_location && that_present_location))
        return false;
      if (!this.location.equals(that.location))
        return false;
    }

    boolean this_present_indexPath = true && this.isSetIndexPath();
    boolean that_present_indexPath = true && that.isSetIndexPath();
    if (this_present_indexPath || that_present_indexPath) {
      if (!(this_present_indexPath && that_present_indexPath))
        return false;
      if (!this.indexPath.equals(that.indexPath))
        return false;
    }

    boolean this_present_partPath = true && this.isSetPartPath();
    boolean that_present_partPath = true && that.isSetPartPath();
    if (this_present_partPath || that_present_partPath) {
      if (!(this_present_partPath && that_present_partPath))
        return false;
      if (!this.partPath.equals(that.partPath))
        return false;
    }

    boolean this_present_type = true;
    boolean that_present_type = true;
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (this.type != that.type)
        return false;
    }

    boolean this_present_status = true;
    boolean that_present_status = true;
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (this.status != that.status)
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
        case DB:
          if (field.type == TType.STRING) {
            this.db = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case TBL:
          if (field.type == TType.STRING) {
            this.tbl = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case NAME:
          if (field.type == TType.STRING) {
            this.name = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case FIELDLIST:
          if (field.type == TType.STRING) {
            this.fieldList = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LOCATION:
          if (field.type == TType.STRING) {
            this.location = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case INDEXPATH:
          if (field.type == TType.STRING) {
            this.indexPath = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PARTPATH:
          if (field.type == TType.SET) {
            {
              TSet _set61 = iprot.readSetBegin();
              this.partPath = new HashSet<String>(2*_set61.size);
              for (int _i62 = 0; _i62 < _set61.size; ++_i62)
              {
                String _elem63;
                _elem63 = iprot.readString();
                this.partPath.add(_elem63);
              }
              iprot.readSetEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case TYPE:
          if (field.type == TType.I32) {
            this.type = iprot.readI32();
            this.__isset.type = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STATUS:
          if (field.type == TType.I32) {
            this.status = iprot.readI32();
            this.__isset.status = true;
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
    if (this.db != null) {
      oprot.writeFieldBegin(DB_FIELD_DESC);
      oprot.writeString(this.db);
      oprot.writeFieldEnd();
    }
    if (this.tbl != null) {
      oprot.writeFieldBegin(TBL_FIELD_DESC);
      oprot.writeString(this.tbl);
      oprot.writeFieldEnd();
    }
    if (this.name != null) {
      oprot.writeFieldBegin(NAME_FIELD_DESC);
      oprot.writeString(this.name);
      oprot.writeFieldEnd();
    }
    if (this.fieldList != null) {
      oprot.writeFieldBegin(FIELD_LIST_FIELD_DESC);
      oprot.writeString(this.fieldList);
      oprot.writeFieldEnd();
    }
    if (this.location != null) {
      oprot.writeFieldBegin(LOCATION_FIELD_DESC);
      oprot.writeString(this.location);
      oprot.writeFieldEnd();
    }
    if (this.indexPath != null) {
      oprot.writeFieldBegin(INDEX_PATH_FIELD_DESC);
      oprot.writeString(this.indexPath);
      oprot.writeFieldEnd();
    }
    if (this.partPath != null) {
      oprot.writeFieldBegin(PART_PATH_FIELD_DESC);
      {
        oprot.writeSetBegin(new TSet(TType.STRING, this.partPath.size()));
        for (String _iter64 : this.partPath)        {
          oprot.writeString(_iter64);
        }
        oprot.writeSetEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(TYPE_FIELD_DESC);
    oprot.writeI32(this.type);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STATUS_FIELD_DESC);
    oprot.writeI32(this.status);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("IndexItem(");
    boolean first = true;

    sb.append("db:");
    if (this.db == null) {
      sb.append("null");
    } else {
      sb.append(this.db);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tbl:");
    if (this.tbl == null) {
      sb.append("null");
    } else {
      sb.append(this.tbl);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("fieldList:");
    if (this.fieldList == null) {
      sb.append("null");
    } else {
      sb.append(this.fieldList);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("location:");
    if (this.location == null) {
      sb.append("null");
    } else {
      sb.append(this.location);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("indexPath:");
    if (this.indexPath == null) {
      sb.append("null");
    } else {
      sb.append(this.indexPath);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("partPath:");
    if (this.partPath == null) {
      sb.append("null");
    } else {
      sb.append(this.partPath);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("type:");
    sb.append(this.type);
    first = false;
    if (!first) sb.append(", ");
    sb.append("status:");
    sb.append(this.status);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}

