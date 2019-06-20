package zipkin.storage.mysql;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SpanName {
  public static class Store {
    public static ConcurrentMap<String, Set<String>> spans = new ConcurrentHashMap<>(16, 0.9f, 2);
  }
}
