package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class DeltaWritableMemChunk implements IWritableMemChunk {
  private long maxTime;
  private IMeasurementSchema schema;
  private TVList stableList;
  private TVList deltaList;
  private DeltaIndexTree deltaTree;

  private static final int DELTA_TREE_DEGREE = 32;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long TARGET_CHUNK_SIZE = CONFIG.getTargetChunkSize();
  private final long MAX_NUMBER_OF_POINTS_IN_CHUNK = CONFIG.getTargetChunkPointNum();

  public DeltaWritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.stableList = TVList.newList(schema.getType());
    this.deltaList = TVList.newList(schema.getType());
    this.deltaTree = new DeltaIndexTree(DELTA_TREE_DEGREE);
    this.maxTime = Long.MIN_VALUE;
  }

  private DeltaWritableMemChunk() {}

  @Override
  public void writeNonAlignedPoint(long insertTime, Object objectValue) {
    switch (schema.getType()) {
      case BOOLEAN:
        putBoolean(insertTime, (boolean) objectValue);
        break;
      case INT32:
      case DATE:
        putInt(insertTime, (int) objectValue);
        break;
      case INT64:
      case TIMESTAMP:
        putLong(insertTime, (long) objectValue);
        break;
      case FLOAT:
        putFloat(insertTime, (float) objectValue);
        break;
      case DOUBLE:
        putDouble(insertTime, (double) objectValue);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        putBinary(insertTime, (Binary) objectValue);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
    }
  }

  @Override
  public void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
  }

  @Override
  public void writeNonAlignedTablet(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        putBooleans(times, boolValues, bitMap, start, end);
        break;
      case INT32:
      case DATE:
        int[] intValues = (int[]) valueList;
        putInts(times, intValues, bitMap, start, end);
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) valueList;
        putLongs(times, longValues, bitMap, start, end);
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        putFloats(times, floatValues, bitMap, start, end);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        putDoubles(times, doubleValues, bitMap, start, end);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) valueList;
        putBinaries(times, binaryValues, bitMap, start, end);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType.name());
    }
  }

  @Override
  public void writeAlignedTablet(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
  }

  @Override
  public void putBoolean(long t, boolean v) {
    if (t >= maxTime) {
      stableList.putBoolean(t, v);
      maxTime = t;
    } else {
      int stableId = stableList.binarySearch(t);
      deltaList.putBoolean(t, v);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t, stableId, deltaId);
    }
  }

  @Override
  public void putInt(long t, int v) {
    if (t >= maxTime) {
      stableList.putInt(t, v);
      maxTime = t;
    } else {
      int stableId = stableList.binarySearch(t);
      deltaList.putInt(t, v);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t, stableId, deltaId);
    }
  }

  @Override
  public void putLong(long t, long v) {
    if (t >= maxTime) {
      stableList.putLong(t, v);
      maxTime = t;
    } else {
      int stableId = stableList.binarySearch(t);
      deltaList.putLong(t, v);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t, stableId, deltaId);
    }
  }

  @Override
  public void putFloat(long t, float v) {
    if (t >= maxTime) {
      stableList.putFloat(t, v);
      maxTime = t;
    } else {
      int stableId = stableList.binarySearch(t);
      deltaList.putFloat(t, v);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t, stableId, deltaId);
    }
  }

  @Override
  public void putDouble(long t, double v) {
    if (t >= maxTime) {
      stableList.putDouble(t, v);
      maxTime = t;
    } else {
      int stableId = stableList.binarySearch(t);
      deltaList.putDouble(t, v);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t, stableId, deltaId);
    }
  }

  @Override
  public void putBinary(long t, Binary v) {
    if (t >= maxTime) {
      stableList.putBinary(t, v);
      maxTime = t;
    } else {
      int stableId = stableList.binarySearch(t);
      deltaList.putBinary(t, v);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t, stableId, deltaId);
    }
  }

  @Override
  public void putAlignedRow(long t, Object[] v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
  }

  private List<Integer> splitStableAndDelta(long[] t, BitMap bitMap, int start, int end) {
    List<Integer> deltaIndices = new ArrayList<>();
    for (int i = start; i < end; i++) {
      if (t[i] >= maxTime) {
        maxTime = t[i];
      } else if (!bitMap.isMarked(i)) {
        deltaIndices.add(i);
        bitMap.mark(i);
      }
    }
    return deltaIndices;
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    List<Integer> deltaIndices = splitStableAndDelta(t, bitMap, start, end);
    stableList.putBooleans(t, v, bitMap, start, end);
    for (int j : deltaIndices) {
      int stableId = stableList.binarySearch(t[j]);
      deltaList.putBoolean(t[j], v[j]);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t[j], stableId, deltaId);
    }
  }

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {
    List<Integer> deltaIndices = splitStableAndDelta(t, bitMap, start, end);
    stableList.putInts(t, v, bitMap, start, end);
    for (int j : deltaIndices) {
      int stableId = stableList.binarySearch(t[j]);
      deltaList.putInt(t[j], v[j]);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t[j], stableId, deltaId);
    }
  }

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {
    List<Integer> deltaIndices = splitStableAndDelta(t, bitMap, start, end);
    stableList.putLongs(t, v, bitMap, start, end);
    for (int j : deltaIndices) {
      int stableId = stableList.binarySearch(t[j]);
      deltaList.putLong(t[j], v[j]);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t[j], stableId, deltaId);
    }
  }

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {
    List<Integer> deltaIndices = splitStableAndDelta(t, bitMap, start, end);
    stableList.putFloats(t, v, bitMap, start, end);
    for (int j : deltaIndices) {
      int stableId = stableList.binarySearch(t[j]);
      deltaList.putFloat(t[j], v[j]);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t[j], stableId, deltaId);
    }
  }

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {
    List<Integer> deltaIndices = splitStableAndDelta(t, bitMap, start, end);
    stableList.putDoubles(t, v, bitMap, start, end);
    for (int j : deltaIndices) {
      int stableId = stableList.binarySearch(t[j]);
      deltaList.putDouble(t[j], v[j]);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t[j], stableId, deltaId);
    }
  }

  @Override
  public void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    List<Integer> deltaIndices = splitStableAndDelta(t, bitMap, start, end);
    stableList.putBinaries(t, v, bitMap, start, end);
    for (int j : deltaIndices) {
      int stableId = stableList.binarySearch(t[j]);
      deltaList.putBinary(t[j], v[j]);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t[j], stableId, deltaId);
    }
  }

  @Override
  public void putAlignedTablet(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void sortTvListForFlush() {
    // do nothing
  }

  @Override
  public long count() {
    return stableList.rowCount() + deltaList.rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TVList getSortedTvListForQuery() {
    return null;
  }

  @Override
  public TVList getSortedTvListForQuery(
      List<IMeasurementSchema> schemaList, boolean ignoreAllNullRows) {
    return null;
  }

  @Override
  public long getMaxTime() {
    return maxTime;
  }

  @Override
  public long getFirstPoint() {
    long minTime = stableList.rowCount() == 0 ? Long.MAX_VALUE : stableList.getTime(0);
    return Math.min(minTime, deltaTree.getMinTime());
  }

  @Override
  public long getLastPoint() {
    return maxTime;
  }

  @Override
  public boolean isEmpty() {
    return stableList.rowCount() == 0 && deltaList.rowCount() == 0;
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    return stableList.delete(lowerBound, upperBound) + deltaList.delete(lowerBound, upperBound);
  }

  @Override
  public ChunkWriterImpl createIChunkWriter() {
    return new ChunkWriterImpl(schema);
  }

  @Override
  public String toString() {
    int size = stableList.rowCount() + deltaList.rowCount();
    if (size == 0) {
      return String.format("MemChunk Size: {}{}" + size + System.lineSeparator());
    }

    TimeValuePair firstPoint = stableList.getTimeValuePair(0);
    TimeValuePair lastPoint = stableList.getTimeValuePair(stableList.rowCount() - 1);
    for (int i = 0; i < deltaList.rowCount(); i++) {
      long currentTime = deltaList.getTime(i);
      if (currentTime < firstPoint.getTimestamp()) {
        firstPoint = deltaList.getTimeValuePair(i);
      }
    }

    return "MemChunk Size: "
        + size
        + System.lineSeparator()
        + "Data type:"
        + schema.getType()
        + System.lineSeparator()
        + "First point:"
        + firstPoint
        + System.lineSeparator()
        + "Last point:"
        + lastPoint
        + System.lineSeparator();
  }

  @Override
  public void encode(BlockingQueue<Object> ioTaskQueue) {
    // todo
  }

  @Override
  public void release() {
    stableList.clear();
    deltaList.clear();
  }

  @Override
  public int serializedSize() {
    return schema.serializedSize()
        + stableList.serializedSize()
        + deltaList.serializedSize()
        + deltaTree.serializedSize();
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    byte[] bytes = new byte[schema.serializedSize()];
    schema.serializeTo(ByteBuffer.wrap(bytes));
    buffer.put(bytes);

    stableList.serializeToWAL(buffer);
    deltaList.serializeToWAL(buffer);
    deltaTree.serializeToWAL(buffer);
  }

  public static DeltaWritableMemChunk deserialize(DataInputStream stream) throws IOException {
    DeltaWritableMemChunk memChunk = new DeltaWritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    memChunk.stableList = TVList.deserialize(stream);
    memChunk.deltaList = TVList.deserialize(stream);
    memChunk.deltaTree = DeltaIndexTree.deserialize(stream);
    return memChunk;
  }
}
