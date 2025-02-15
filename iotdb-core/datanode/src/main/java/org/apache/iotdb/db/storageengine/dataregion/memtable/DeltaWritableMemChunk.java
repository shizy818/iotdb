package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;
import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

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
    if (bitMap == null) {
      bitMap = new BitMap(t.length);
    }
    List<Integer> deltaIndices = new ArrayList<>();
    for (int i = start; i < end; i++) {
      if (bitMap.isMarked(i)) {
        continue;
      }
      if (t[i] >= maxTime) {
        maxTime = t[i];
      } else {
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
    return stableList.count() + deltaList.count();
  }

  @Override
  public long rowCount() {
    return stableList.rowCount() + deltaList.rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery() {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
  }

  @Override
  public synchronized TVList getSortedTvListForQuery(
      List<IMeasurementSchema> schemaList, boolean ignoreAllNullRows) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
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
    int delete1 = stableList.delete(lowerBound, upperBound);
    int delete2 = deltaList.delete(lowerBound, upperBound);
    return delete1 + delete2;
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

  private long writeData(
      ChunkWriterImpl chunkWriterImpl,
      TSDataType tsDataType,
      TimeValuePair tvPair,
      long dataSizeInCurrentChunk) {
    long time = tvPair.getTimestamp();
    switch (tsDataType) {
      case BOOLEAN:
        chunkWriterImpl.write(time, tvPair.getValue().getBoolean());
        dataSizeInCurrentChunk += 8L + 1L;
        break;
      case INT32:
      case DATE:
        chunkWriterImpl.write(time, tvPair.getValue().getInt());
        dataSizeInCurrentChunk += 8L + 4L;
        break;
      case INT64:
      case TIMESTAMP:
        chunkWriterImpl.write(time, tvPair.getValue().getLong());
        dataSizeInCurrentChunk += 8L + 8L;
        break;
      case FLOAT:
        chunkWriterImpl.write(time, tvPair.getValue().getFloat());
        dataSizeInCurrentChunk += 8L + 4L;
        break;
      case DOUBLE:
        chunkWriterImpl.write(time, tvPair.getValue().getDouble());
        dataSizeInCurrentChunk += 8L + 8L;
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary value = tvPair.getValue().getBinary();
        chunkWriterImpl.write(time, value);
        dataSizeInCurrentChunk += 8L + getBinarySize(value);
        break;
      default:
        LOGGER.error("WritableMemChunk does not support data type: {}", tsDataType);
        break;
    }
    return dataSizeInCurrentChunk;
  }

  @Override
  public void encode(BlockingQueue<Object> ioTaskQueue) {
    TSDataType tsDataType = schema.getType();
    ChunkWriterImpl chunkWriterImpl = createIChunkWriter();
    long dataSizeInCurrentChunk = 0;
    int pointNumInCurrentChunk = 0;

    DeltaMemChunkIterator iterator = iterator();
    TimeValuePair prevTvPair = null;
    while (iterator.hasNext()) {
      TimeValuePair tvPair = iterator.next();
      if (prevTvPair == null) {
        prevTvPair = tvPair;
        continue;
      }

      dataSizeInCurrentChunk =
          writeData(chunkWriterImpl, tsDataType, prevTvPair, dataSizeInCurrentChunk);
      pointNumInCurrentChunk++;
      prevTvPair = tvPair;

      if (pointNumInCurrentChunk > MAX_NUMBER_OF_POINTS_IN_CHUNK
          || dataSizeInCurrentChunk > TARGET_CHUNK_SIZE) {
        chunkWriterImpl.sealCurrentPage();
        chunkWriterImpl.clearPageWriter();
        try {
          ioTaskQueue.put(chunkWriterImpl);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        chunkWriterImpl = createIChunkWriter();
        dataSizeInCurrentChunk = 0;
        pointNumInCurrentChunk = 0;
      }
    }

    // last point for SDT
    if (prevTvPair != null) {
      chunkWriterImpl.setLastPoint(true);
      writeData(chunkWriterImpl, tsDataType, prevTvPair, dataSizeInCurrentChunk);
      pointNumInCurrentChunk++;
    }
    if (pointNumInCurrentChunk != 0) {
      chunkWriterImpl.sealCurrentPage();
      chunkWriterImpl.clearPageWriter();
      try {
        ioTaskQueue.put(chunkWriterImpl);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void release() {
    stableList.clear();
    deltaList.clear();
    deltaTree.clear();
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

  public TsBlock buildTsBlock(
      int floatPrecision, TSEncoding encoding, List<TimeRange> deletionList) {
    TSDataType tsDataType = schema.getType();
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(tsDataType));
    DeltaMemChunkIterator iterator = iterator();
    int[] deleteCursor = {0};
    while (iterator.hasNext()) {
      TimeValuePair tvPair = iterator.next();
      long time = tvPair.getTimestamp();
      if (!isPointDeleted(time, deletionList, deleteCursor)) {
        builder.getTimeColumnBuilder().writeLong(time);
        switch (tsDataType) {
          case BOOLEAN:
            builder.getColumnBuilder(0).writeBoolean(tvPair.getValue().getBoolean());
            break;
          case INT32:
          case DATE:
            builder.getColumnBuilder(0).writeInt(tvPair.getValue().getInt());
            break;
          case INT64:
          case TIMESTAMP:
            builder.getColumnBuilder(0).writeLong(tvPair.getValue().getLong());
            break;
          case FLOAT:
            float fv = tvPair.getValue().getFloat();
            if (!Float.isNaN(fv)
                && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
              fv = MathUtils.roundWithGivenPrecision(fv, floatPrecision);
            }
            builder.getColumnBuilder(0).writeFloat(fv);
            break;
          case DOUBLE:
            double dv = tvPair.getValue().getDouble();
            if (!Double.isNaN(dv)
                && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
              dv = MathUtils.roundWithGivenPrecision(dv, floatPrecision);
            }
            builder.getColumnBuilder(0).writeDouble(dv);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            builder.getColumnBuilder(0).writeBinary(tvPair.getValue().getBinary());
            break;
          default:
            LOGGER.error("buildTsBlock does not support data type: {}", tsDataType);
            break;
        }
        builder.declarePosition();
      }
    }
    return builder.build();
  }

  public static DeltaWritableMemChunk deserialize(DataInputStream stream) throws IOException {
    DeltaWritableMemChunk memChunk = new DeltaWritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    memChunk.stableList = TVList.deserialize(stream);
    memChunk.deltaList = TVList.deserialize(stream);
    memChunk.deltaTree = DeltaIndexTree.deserialize(stream);
    return memChunk;
  }

  public DeltaMemChunkIterator iterator() {
    return new DeltaMemChunkIterator();
  }

  public class DeltaMemChunkIterator {
    private DeltaIndexTree.DeltaIndexTreeLeafNode current;
    private int entryIndex = 0;
    private int stableIndex = 0;

    private boolean probeNext = false;
    private TimeValuePair currentTvPair = null;

    public DeltaMemChunkIterator() {
      this.current = deltaTree.getFirstLeaf();
    }

    private void nextDeltaEntry() {
      entryIndex++;
      // next leaf page
      if (entryIndex >= current.count) {
        current = current.next;
        entryIndex = 0;
      }
    }

    private void prepareNext() {
      currentTvPair = null;
      boolean validEntry = false;

      // skip deleted entry
      while (current != null && entryIndex < current.count) {
        validEntry = true;
        if (deltaList.isNullValue(current.deltaIds[entryIndex])) {
          validEntry = false;
          nextDeltaEntry();
        } else {
          break;
        }
      }
      // skip duplicated timestamp
      while (current != null && entryIndex < current.count) {
        if ((entryIndex + 1 < current.count
                && current.keys[entryIndex] == current.keys[entryIndex + 1])
            || (current.next != null
                && !current.next.isEmpty()
                && current.keys[entryIndex] == current.next.keys[0])) {
          nextDeltaEntry();
        } else {
          break;
        }
      }

      int stableRowCount = stableList.rowCount();
      if (validEntry) {
        int stableId = current.stableIds[entryIndex];
        int deltaId = current.deltaIds[entryIndex];
        // skip deleted rows
        while (stableIndex <= stableId && stableList.isNullValue(stableIndex)) {
          stableIndex++;
        }
        // skip duplicated timestamp
        while (stableIndex + 1 <= stableId
            && stableList.getTime(stableIndex) == stableList.getTime(stableIndex + 1)) {
          stableIndex++;
        }

        if (stableIndex > stableId) {
          currentTvPair = deltaList.getTimeValuePair(deltaId);
        } else if (stableIndex == stableId
            && stableList.getTime(stableIndex) == deltaList.getTime(deltaId)) {
          currentTvPair = deltaList.getTimeValuePair(deltaId);
          stableIndex++;
        } else {
          currentTvPair = stableList.getTimeValuePair(stableIndex);
        }
      } else {
        // skip deleted rows
        while (stableIndex < stableRowCount && stableList.isNullValue(stableIndex)) {
          stableIndex++;
        }
        // skip duplicated timestamp
        while (stableIndex + 1 < stableRowCount
            && stableList.getTime(stableIndex) == stableList.getTime(stableIndex + 1)) {
          stableIndex++;
        }
        if (stableIndex < stableRowCount) {
          currentTvPair = stableList.getTimeValuePair(stableIndex);
        }
      }
      probeNext = true;
    }

    public boolean hasNext() {
      if (!probeNext) {
        prepareNext();
      }
      return currentTvPair != null;
    }

    public TimeValuePair next() {
      if (!hasNext()) {
        return null;
      }
      if (current == null || current.isEmpty()) {
        stableIndex++;
      } else {
        int stableId = current.stableIds[entryIndex];
        if (stableIndex <= stableId) {
          stableIndex++;
        } else {
          nextDeltaEntry();
        }
      }
      probeNext = false;
      return currentTvPair;
    }
  }
}
