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

public class Schema implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("Schema");
  private static final TField FIELD_SCHEMAS_FIELD_DESC = new TField("fieldSchemas", TType.LIST, (short)1);
  private static final TField PROPERTIES_FIELD_DESC = new TField("properties", TType.MAP, (short)2);

  private List<FieldSchema> fieldSchemas;
  public static final int FIELDSCHEMAS = 1;
  private Map<String,String> properties;
  public static final int PROPERTIES = 2;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(FIELDSCHEMAS, new FieldMetaData("fieldSchemas", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new StructMetaData(TType.STRUCT, FieldSchema.class))));
    put(PROPERTIES, new FieldMetaData("properties", TFieldRequirementType.DEFAULT, 
        new MapMetaData(TType.MAP, 
            new FieldValueMetaData(TType.STRING), 
            new FieldValueMetaData(TType.STRING))));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(Schema.class, metaDataMap);
  }

  public Schema() {
  }

  public Schema(
    List<FieldSchema> fieldSchemas,
    Map<String,String> properties)
  {
    this();
    this.fieldSchemas = fieldSchemas;
    this.properties = properties;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Schema(Schema other) {
    if (other.isSetFieldSchemas()) {
      List<FieldSchema> __this__fieldSchemas = new ArrayList<FieldSchema>();
      for (FieldSchema other_element : other.fieldSchemas) {
        __this__fieldSchemas.add(new FieldSchema(other_element));
      }
      this.fieldSchemas = __this__fieldSchemas;
    }
    if (other.isSetProperties()) {
      Map<String,String> __this__properties = new HashMap<String,String>();
      for (Map.Entry<String, String> other_element : other.properties.entrySet()) {

        String other_element_key = other_element.getKey();
        String other_element_value = other_element.getValue();

        String __this__properties_copy_key = other_element_key;

        String __this__properties_copy_value = other_element_value;

        __this__properties.put(__this__properties_copy_key, __this__properties_copy_value);
      }
      this.properties = __this__properties;
    }
  }

  @Override
  public Schema clone() {
    return new Schema(this);
  }

  public int getFieldSchemasSize() {
    return (this.fieldSchemas == null) ? 0 : this.fieldSchemas.size();
  }

  public java.util.Iterator<FieldSchema> getFieldSchemasIterator() {
    return (this.fieldSchemas == null) ? null : this.fieldSchemas.iterator();
  }

  public void addToFieldSchemas(FieldSchema elem) {
    if (this.fieldSchemas == null) {
      this.fieldSchemas = new ArrayList<FieldSchema>();
    }
    this.fieldSchemas.add(elem);
  }

  public List<FieldSchema> getFieldSchemas() {
    return this.fieldSchemas;
  }

  public void setFieldSchemas(List<FieldSchema> fieldSchemas) {
    this.fieldSchemas = fieldSchemas;
  }

  public void unsetFieldSchemas() {
    this.fieldSchemas = null;
  }

  // Returns true if field fieldSchemas is set (has been asigned a value) and false otherwise
  public boolean isSetFieldSchemas() {
    return this.fieldSchemas != null;
  }

  public int getPropertiesSize() {
    return (this.properties == null) ? 0 : this.properties.size();
  }

  public void putToProperties(String key, String val) {
    if (this.properties == null) {
      this.properties = new HashMap<String,String>();
    }
    this.properties.put(key, val);
  }

  public Map<String,String> getProperties() {
    return this.properties;
  }

  public void setProperties(Map<String,String> properties) {
    this.properties = properties;
  }

  public void unsetProperties() {
    this.properties = null;
  }

  // Returns true if field properties is set (has been asigned a value) and false otherwise
  public boolean isSetProperties() {
    return this.properties != null;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case FIELDSCHEMAS:
      if (value == null) {
        unsetFieldSchemas();
      } else {
        setFieldSchemas((List<FieldSchema>)value);
      }
      break;

    case PROPERTIES:
      if (value == null) {
        unsetProperties();
      } else {
        setProperties((Map<String,String>)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case FIELDSCHEMAS:
      return getFieldSchemas();

    case PROPERTIES:
      return getProperties();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case FIELDSCHEMAS:
      return isSetFieldSchemas();
    case PROPERTIES:
      return isSetProperties();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Schema)
      return this.equals((Schema)that);
    return false;
  }

  public boolean equals(Schema that) {
    if (that == null)
      return false;

    boolean this_present_fieldSchemas = true && this.isSetFieldSchemas();
    boolean that_present_fieldSchemas = true && that.isSetFieldSchemas();
    if (this_present_fieldSchemas || that_present_fieldSchemas) {
      if (!(this_present_fieldSchemas && that_present_fieldSchemas))
        return false;
      if (!this.fieldSchemas.equals(that.fieldSchemas))
        return false;
    }

    boolean this_present_properties = true && this.isSetProperties();
    boolean that_present_properties = true && that.isSetProperties();
    if (this_present_properties || that_present_properties) {
      if (!(this_present_properties && that_present_properties))
        return false;
      if (!this.properties.equals(that.properties))
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
        case FIELDSCHEMAS:
          if (field.type == TType.LIST) {
            {
              TList _list44 = iprot.readListBegin();
              this.fieldSchemas = new ArrayList<FieldSchema>(_list44.size);
              for (int _i45 = 0; _i45 < _list44.size; ++_i45)
              {
                FieldSchema _elem46;
                _elem46 = new FieldSchema();
                _elem46.read(iprot);
                this.fieldSchemas.add(_elem46);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PROPERTIES:
          if (field.type == TType.MAP) {
            {
              TMap _map47 = iprot.readMapBegin();
              this.properties = new HashMap<String,String>(2*_map47.size);
              for (int _i48 = 0; _i48 < _map47.size; ++_i48)
              {
                String _key49;
                String _val50;
                _key49 = iprot.readString();
                _val50 = iprot.readString();
                this.properties.put(_key49, _val50);
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
    if (this.fieldSchemas != null) {
      oprot.writeFieldBegin(FIELD_SCHEMAS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRUCT, this.fieldSchemas.size()));
        for (FieldSchema _iter51 : this.fieldSchemas)        {
          _iter51.write(oprot);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.properties != null) {
      oprot.writeFieldBegin(PROPERTIES_FIELD_DESC);
      {
        oprot.writeMapBegin(new TMap(TType.STRING, TType.STRING, this.properties.size()));
        for (Map.Entry<String, String> _iter52 : this.properties.entrySet())        {
          oprot.writeString(_iter52.getKey());
          oprot.writeString(_iter52.getValue());
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
    StringBuilder sb = new StringBuilder("Schema(");
    boolean first = true;

    sb.append("fieldSchemas:");
    if (this.fieldSchemas == null) {
      sb.append("null");
    } else {
      sb.append(this.fieldSchemas);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("properties:");
    if (this.properties == null) {
      sb.append("null");
    } else {
      sb.append(this.properties);
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

