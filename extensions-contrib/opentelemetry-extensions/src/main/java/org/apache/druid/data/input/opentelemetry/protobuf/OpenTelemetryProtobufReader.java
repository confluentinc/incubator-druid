package org.apache.druid.data.input.opentelemetry.protobuf;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Iterator;

public abstract class OpenTelemetryProtobufReader implements InputEntityReader {
  public OpenTelemetryProtobufReader(SettableByteEntity<? extends ByteEntity> source)
  {
    this.source = source;
  }

  protected SettableByteEntity<? extends ByteEntity> source;

  @Override
  public CloseableIterator<InputRow> read()
  {
    Supplier<Iterator<InputRow>> supplier = Suppliers.memoize(() -> readAsList().iterator());
    return CloseableIterators.withEmptyBaggage(new Iterator<InputRow>() {
      @Override
      public boolean hasNext()
      {
        return supplier.get().hasNext();
      }
      @Override
      public InputRow next()
      {
        return supplier.get().next();
      }
    });
  }

  List<InputRow> readAsList()
  {
    try {
      ByteBuffer buffer = source.getEntity().getBuffer();
      List<InputRow> rows = getRows(buffer);
      // Explicitly move the position assuming that all the remaining bytes have been consumed because the protobuf
      // parser does not update the position itself
      buffer.position(buffer.limit());
      return rows;
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(null, e, "Protobuf message could not be parsed");
    }
  }

  public abstract List<InputRow> getRows(ByteBuffer byteBuffer) throws InvalidProtocolBufferException;

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}
