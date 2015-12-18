/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#ifndef hive_metastore_TYPES_H
#define hive_metastore_TYPES_H

#include <Thrift.h>
#include <protocol/TProtocol.h>
#include <transport/TTransport.h>

#include "fb303_types.h"


namespace Apache { namespace Hadoop { namespace Hive {

class Version {
 public:

  static const char* ascii_fingerprint; // = "07A9615F837F7D0A952B595DD3020972";
  static const uint8_t binary_fingerprint[16]; // = {0x07,0xA9,0x61,0x5F,0x83,0x7F,0x7D,0x0A,0x95,0x2B,0x59,0x5D,0xD3,0x02,0x09,0x72};

  Version() : version(""), comments("") {
  }

  virtual ~Version() throw() {}

  std::string version;
  std::string comments;

  struct __isset {
    __isset() : version(false), comments(false) {}
    bool version;
    bool comments;
  } __isset;

  bool operator == (const Version & rhs) const
  {
    if (!(version == rhs.version))
      return false;
    if (!(comments == rhs.comments))
      return false;
    return true;
  }
  bool operator != (const Version &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Version & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class FieldSchema {
 public:

  static const char* ascii_fingerprint; // = "AB879940BD15B6B25691265F7384B271";
  static const uint8_t binary_fingerprint[16]; // = {0xAB,0x87,0x99,0x40,0xBD,0x15,0xB6,0xB2,0x56,0x91,0x26,0x5F,0x73,0x84,0xB2,0x71};

  FieldSchema() : name(""), type(""), comment("") {
  }

  virtual ~FieldSchema() throw() {}

  std::string name;
  std::string type;
  std::string comment;

  struct __isset {
    __isset() : name(false), type(false), comment(false) {}
    bool name;
    bool type;
    bool comment;
  } __isset;

  bool operator == (const FieldSchema & rhs) const
  {
    if (!(name == rhs.name))
      return false;
    if (!(type == rhs.type))
      return false;
    if (!(comment == rhs.comment))
      return false;
    return true;
  }
  bool operator != (const FieldSchema &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const FieldSchema & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class Type {
 public:

  static const char* ascii_fingerprint; // = "20DF02DE523C27F7066C7BD4D9120842";
  static const uint8_t binary_fingerprint[16]; // = {0x20,0xDF,0x02,0xDE,0x52,0x3C,0x27,0xF7,0x06,0x6C,0x7B,0xD4,0xD9,0x12,0x08,0x42};

  Type() : name(""), type1(""), type2("") {
  }

  virtual ~Type() throw() {}

  std::string name;
  std::string type1;
  std::string type2;
  std::vector<FieldSchema>  fields;

  struct __isset {
    __isset() : name(false), type1(false), type2(false), fields(false) {}
    bool name;
    bool type1;
    bool type2;
    bool fields;
  } __isset;

  bool operator == (const Type & rhs) const
  {
    if (!(name == rhs.name))
      return false;
    if (__isset.type1 != rhs.__isset.type1)
      return false;
    else if (__isset.type1 && !(type1 == rhs.type1))
      return false;
    if (__isset.type2 != rhs.__isset.type2)
      return false;
    else if (__isset.type2 && !(type2 == rhs.type2))
      return false;
    if (__isset.fields != rhs.__isset.fields)
      return false;
    else if (__isset.fields && !(fields == rhs.fields))
      return false;
    return true;
  }
  bool operator != (const Type &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Type & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class Database {
 public:

  static const char* ascii_fingerprint; // = "07A9615F837F7D0A952B595DD3020972";
  static const uint8_t binary_fingerprint[16]; // = {0x07,0xA9,0x61,0x5F,0x83,0x7F,0x7D,0x0A,0x95,0x2B,0x59,0x5D,0xD3,0x02,0x09,0x72};

  Database() : name(""), description("") {
  }

  virtual ~Database() throw() {}

  std::string name;
  std::string description;

  struct __isset {
    __isset() : name(false), description(false) {}
    bool name;
    bool description;
  } __isset;

  bool operator == (const Database & rhs) const
  {
    if (!(name == rhs.name))
      return false;
    if (!(description == rhs.description))
      return false;
    return true;
  }
  bool operator != (const Database &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Database & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class SerDeInfo {
 public:

  static const char* ascii_fingerprint; // = "B1021C32A35A2AEFCD2F57A5424159A7";
  static const uint8_t binary_fingerprint[16]; // = {0xB1,0x02,0x1C,0x32,0xA3,0x5A,0x2A,0xEF,0xCD,0x2F,0x57,0xA5,0x42,0x41,0x59,0xA7};

  SerDeInfo() : name(""), serializationLib("") {
  }

  virtual ~SerDeInfo() throw() {}

  std::string name;
  std::string serializationLib;
  std::map<std::string, std::string>  parameters;

  struct __isset {
    __isset() : name(false), serializationLib(false), parameters(false) {}
    bool name;
    bool serializationLib;
    bool parameters;
  } __isset;

  bool operator == (const SerDeInfo & rhs) const
  {
    if (!(name == rhs.name))
      return false;
    if (!(serializationLib == rhs.serializationLib))
      return false;
    if (!(parameters == rhs.parameters))
      return false;
    return true;
  }
  bool operator != (const SerDeInfo &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const SerDeInfo & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class Order {
 public:

  static const char* ascii_fingerprint; // = "EEBC915CE44901401D881E6091423036";
  static const uint8_t binary_fingerprint[16]; // = {0xEE,0xBC,0x91,0x5C,0xE4,0x49,0x01,0x40,0x1D,0x88,0x1E,0x60,0x91,0x42,0x30,0x36};

  Order() : col(""), order(0) {
  }

  virtual ~Order() throw() {}

  std::string col;
  int32_t order;

  struct __isset {
    __isset() : col(false), order(false) {}
    bool col;
    bool order;
  } __isset;

  bool operator == (const Order & rhs) const
  {
    if (!(col == rhs.col))
      return false;
    if (!(order == rhs.order))
      return false;
    return true;
  }
  bool operator != (const Order &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Order & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class StorageDescriptor {
 public:

  static const char* ascii_fingerprint; // = "11E4CE18F895C13812C853DFDCD1293F";
  static const uint8_t binary_fingerprint[16]; // = {0x11,0xE4,0xCE,0x18,0xF8,0x95,0xC1,0x38,0x12,0xC8,0x53,0xDF,0xDC,0xD1,0x29,0x3F};

  StorageDescriptor() : location(""), inputFormat(""), outputFormat(""), compressed(0), numBuckets(0) {
  }

  virtual ~StorageDescriptor() throw() {}

  std::vector<FieldSchema>  cols;
  std::string location;
  std::string inputFormat;
  std::string outputFormat;
  bool compressed;
  int32_t numBuckets;
  SerDeInfo serdeInfo;
  std::vector<std::string>  bucketCols;
  std::vector<Order>  sortCols;
  std::map<std::string, std::string>  parameters;

  struct __isset {
    __isset() : cols(false), location(false), inputFormat(false), outputFormat(false), compressed(false), numBuckets(false), serdeInfo(false), bucketCols(false), sortCols(false), parameters(false) {}
    bool cols;
    bool location;
    bool inputFormat;
    bool outputFormat;
    bool compressed;
    bool numBuckets;
    bool serdeInfo;
    bool bucketCols;
    bool sortCols;
    bool parameters;
  } __isset;

  bool operator == (const StorageDescriptor & rhs) const
  {
    if (!(cols == rhs.cols))
      return false;
    if (!(location == rhs.location))
      return false;
    if (!(inputFormat == rhs.inputFormat))
      return false;
    if (!(outputFormat == rhs.outputFormat))
      return false;
    if (!(compressed == rhs.compressed))
      return false;
    if (!(numBuckets == rhs.numBuckets))
      return false;
    if (!(serdeInfo == rhs.serdeInfo))
      return false;
    if (!(bucketCols == rhs.bucketCols))
      return false;
    if (!(sortCols == rhs.sortCols))
      return false;
    if (!(parameters == rhs.parameters))
      return false;
    return true;
  }
  bool operator != (const StorageDescriptor &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const StorageDescriptor & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class Partition {
 public:

  static const char* ascii_fingerprint; // = "8D17C3980D5D53A4766ACCB56A44A83B";
  static const uint8_t binary_fingerprint[16]; // = {0x8D,0x17,0xC3,0x98,0x0D,0x5D,0x53,0xA4,0x76,0x6A,0xCC,0xB5,0x6A,0x44,0xA8,0x3B};

  Partition() : dbName(""), tableName(""), level(0), parType("") {
  }

  virtual ~Partition() throw() {}

  std::string dbName;
  std::string tableName;
  int32_t level;
  std::string parType;
  FieldSchema parKey;
  std::map<std::string, std::vector<std::string> >  parSpaces;

  struct __isset {
    __isset() : dbName(false), tableName(false), level(false), parType(false), parKey(false), parSpaces(false) {}
    bool dbName;
    bool tableName;
    bool level;
    bool parType;
    bool parKey;
    bool parSpaces;
  } __isset;

  bool operator == (const Partition & rhs) const
  {
    if (!(dbName == rhs.dbName))
      return false;
    if (!(tableName == rhs.tableName))
      return false;
    if (!(level == rhs.level))
      return false;
    if (!(parType == rhs.parType))
      return false;
    if (!(parKey == rhs.parKey))
      return false;
    if (!(parSpaces == rhs.parSpaces))
      return false;
    return true;
  }
  bool operator != (const Partition &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Partition & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class Table {
 public:

  static const char* ascii_fingerprint; // = "61100BFC055642B6384526B36DF999E9";
  static const uint8_t binary_fingerprint[16]; // = {0x61,0x10,0x0B,0xFC,0x05,0x56,0x42,0xB6,0x38,0x45,0x26,0xB3,0x6D,0xF9,0x99,0xE9};

  Table() : tableName(""), dbName(""), owner(""), createTime(0), lastAccessTime(0), retention(0) {
  }

  virtual ~Table() throw() {}

  std::string tableName;
  std::string dbName;
  std::string owner;
  int32_t createTime;
  int32_t lastAccessTime;
  int32_t retention;
  StorageDescriptor sd;
  Partition priPartition;
  Partition subPartition;
  std::map<std::string, std::string>  parameters;

  struct __isset {
    __isset() : tableName(false), dbName(false), owner(false), createTime(false), lastAccessTime(false), retention(false), sd(false), priPartition(false), subPartition(false), parameters(false) {}
    bool tableName;
    bool dbName;
    bool owner;
    bool createTime;
    bool lastAccessTime;
    bool retention;
    bool sd;
    bool priPartition;
    bool subPartition;
    bool parameters;
  } __isset;

  bool operator == (const Table & rhs) const
  {
    if (!(tableName == rhs.tableName))
      return false;
    if (!(dbName == rhs.dbName))
      return false;
    if (!(owner == rhs.owner))
      return false;
    if (!(createTime == rhs.createTime))
      return false;
    if (!(lastAccessTime == rhs.lastAccessTime))
      return false;
    if (!(retention == rhs.retention))
      return false;
    if (!(sd == rhs.sd))
      return false;
    if (!(priPartition == rhs.priPartition))
      return false;
    if (!(subPartition == rhs.subPartition))
      return false;
    if (!(parameters == rhs.parameters))
      return false;
    return true;
  }
  bool operator != (const Table &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Table & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class Index {
 public:

  static const char* ascii_fingerprint; // = "3163EDEDA2214D868610157908B1AB7A";
  static const uint8_t binary_fingerprint[16]; // = {0x31,0x63,0xED,0xED,0xA2,0x21,0x4D,0x86,0x86,0x10,0x15,0x79,0x08,0xB1,0xAB,0x7A};

  Index() : indexName(""), indexType(0), tableName(""), dbName(""), partName("") {
  }

  virtual ~Index() throw() {}

  std::string indexName;
  int32_t indexType;
  std::string tableName;
  std::string dbName;
  std::vector<std::string>  colNames;
  std::string partName;

  struct __isset {
    __isset() : indexName(false), indexType(false), tableName(false), dbName(false), colNames(false), partName(false) {}
    bool indexName;
    bool indexType;
    bool tableName;
    bool dbName;
    bool colNames;
    bool partName;
  } __isset;

  bool operator == (const Index & rhs) const
  {
    if (!(indexName == rhs.indexName))
      return false;
    if (!(indexType == rhs.indexType))
      return false;
    if (!(tableName == rhs.tableName))
      return false;
    if (!(dbName == rhs.dbName))
      return false;
    if (!(colNames == rhs.colNames))
      return false;
    if (!(partName == rhs.partName))
      return false;
    return true;
  }
  bool operator != (const Index &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Index & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class Schema {
 public:

  static const char* ascii_fingerprint; // = "5CFEE46C975F4E2368D905109B8E3B5B";
  static const uint8_t binary_fingerprint[16]; // = {0x5C,0xFE,0xE4,0x6C,0x97,0x5F,0x4E,0x23,0x68,0xD9,0x05,0x10,0x9B,0x8E,0x3B,0x5B};

  Schema() {
  }

  virtual ~Schema() throw() {}

  std::vector<FieldSchema>  fieldSchemas;
  std::map<std::string, std::string>  properties;

  struct __isset {
    __isset() : fieldSchemas(false), properties(false) {}
    bool fieldSchemas;
    bool properties;
  } __isset;

  bool operator == (const Schema & rhs) const
  {
    if (!(fieldSchemas == rhs.fieldSchemas))
      return false;
    if (!(properties == rhs.properties))
      return false;
    return true;
  }
  bool operator != (const Schema &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Schema & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class MetaException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  MetaException() : message("") {
  }

  virtual ~MetaException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const MetaException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const MetaException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const MetaException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class UnknownTableException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  UnknownTableException() : message("") {
  }

  virtual ~UnknownTableException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const UnknownTableException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const UnknownTableException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const UnknownTableException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class UnknownDBException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  UnknownDBException() : message("") {
  }

  virtual ~UnknownDBException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const UnknownDBException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const UnknownDBException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const UnknownDBException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class AlreadyExistsException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  AlreadyExistsException() : message("") {
  }

  virtual ~AlreadyExistsException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const AlreadyExistsException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const AlreadyExistsException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const AlreadyExistsException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class InvalidObjectException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  InvalidObjectException() : message("") {
  }

  virtual ~InvalidObjectException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const InvalidObjectException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const InvalidObjectException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const InvalidObjectException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class NoSuchObjectException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  NoSuchObjectException() : message("") {
  }

  virtual ~NoSuchObjectException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const NoSuchObjectException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const NoSuchObjectException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const NoSuchObjectException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class IndexAlreadyExistsException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  IndexAlreadyExistsException() : message("") {
  }

  virtual ~IndexAlreadyExistsException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const IndexAlreadyExistsException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const IndexAlreadyExistsException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const IndexAlreadyExistsException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

class InvalidOperationException : public apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "EFB929595D312AC8F305D5A794CFEDA1";
  static const uint8_t binary_fingerprint[16]; // = {0xEF,0xB9,0x29,0x59,0x5D,0x31,0x2A,0xC8,0xF3,0x05,0xD5,0xA7,0x94,0xCF,0xED,0xA1};

  InvalidOperationException() : message("") {
  }

  virtual ~InvalidOperationException() throw() {}

  std::string message;

  struct __isset {
    __isset() : message(false) {}
    bool message;
  } __isset;

  bool operator == (const InvalidOperationException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    return true;
  }
  bool operator != (const InvalidOperationException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const InvalidOperationException & ) const;

  uint32_t read(apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(apache::thrift::protocol::TProtocol* oprot) const;

};

}}} // namespace

#endif
