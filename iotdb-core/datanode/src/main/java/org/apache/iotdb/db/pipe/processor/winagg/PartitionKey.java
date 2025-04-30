package org.apache.iotdb.db.pipe.processor.winagg;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PartitionKey implements Comparable<PartitionKey> {
  private final List<Comparable<Object>> keys = new ArrayList<>();

  public PartitionKey(Comparable<Object>... objects) {
    Collections.addAll(keys, objects);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();
    for (Comparable<Object> key : keys) {
      builder.append(key);
    }
    return builder.toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionKey p = (PartitionKey) o;
    if (this.keys.size() != p.keys.size()) {
      return false;
    }

    for (int i = 0; i < this.keys.size(); i++) {
      Comparable<Object> o1 = this.keys.get(i);
      Comparable<Object> o2 = p.keys.get(i);
      if (o1.getClass() != o2.getClass() || o1.equals(o2)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(PartitionKey p) {
    if (this.keys.size() != p.keys.size()) {
      return this.keys.size() > p.keys.size() ? 1 : -1;
    }

    for (int i = 0; i < this.keys.size(); i++) {
      Comparable<Object> o1 = this.keys.get(i);
      Comparable<Object> o2 = p.keys.get(i);
      if (o1.equals(o2)) {
        continue;
      }
      return o1.compareTo(o2);
    }
    return 0;
  }
}
