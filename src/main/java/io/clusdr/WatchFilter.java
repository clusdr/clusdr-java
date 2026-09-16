package io.clusdr;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/** Optional Watch filter. Empty (default) is the full bus. */
public final class WatchFilter {
  private final List<String> topics;
  private final List<String> eventTypes;

  private WatchFilter(List<String> topics, List<String> eventTypes) {
    this.topics = List.copyOf(topics);
    this.eventTypes = List.copyOf(eventTypes);
  }

  public static WatchFilter all() {
    return new WatchFilter(List.of(), List.of());
  }

  public WatchFilter topics(String... topics) {
    return new WatchFilter(normalizeTopics(List.of(topics)), eventTypes);
  }

  public WatchFilter topics(Iterable<String> topics) {
    List<String> list = new ArrayList<>();
    for (String t : topics) {
      list.add(t);
    }
    return new WatchFilter(normalizeTopics(list), eventTypes);
  }

  public WatchFilter eventTypes(String... eventTypes) {
    return new WatchFilter(topics, normalizeTypes(List.of(eventTypes)));
  }

  public WatchFilter eventTypes(Iterable<String> eventTypes) {
    List<String> list = new ArrayList<>();
    for (String t : eventTypes) {
      list.add(t);
    }
    return new WatchFilter(topics, normalizeTypes(list));
  }

  List<String> topics() {
    return topics;
  }

  List<String> eventTypes() {
    return eventTypes;
  }

  static WatchFilter normalize(WatchFilter filter) {
    if (filter == null) {
      return all();
    }
    return new WatchFilter(normalizeTopics(filter.topics), normalizeTypes(filter.eventTypes));
  }

  private static List<String> normalizeTopics(List<String> topics) {
    if (topics == null || topics.isEmpty()) {
      return List.of();
    }
    LinkedHashSet<String> seen = new LinkedHashSet<>();
    for (String raw : topics) {
      if (raw == null) {
        continue;
      }
      String t = raw.trim();
      if (t.startsWith("custom.")) {
        t = t.substring(7);
      }
      if (t.isEmpty()) {
        continue;
      }
      if (t.length() > Options.MAX_WATCH_TOPIC || !validTopic(t)) {
        throw new ClusdrException("clusdr: watch topic " + json(raw) + " is invalid");
      }
      seen.add(t);
    }
    return List.copyOf(seen);
  }

  private static List<String> normalizeTypes(List<String> types) {
    if (types == null || types.isEmpty()) {
      return List.of();
    }
    LinkedHashSet<String> seen = new LinkedHashSet<>();
    for (String raw : types) {
      if (raw == null) {
        continue;
      }
      String t = raw.trim();
      if (t.isEmpty()) {
        continue;
      }
      seen.add(t);
    }
    return List.copyOf(seen);
  }

  private static boolean validTopic(String t) {
    for (int i = 0; i < t.length(); i++) {
      char c = t.charAt(i);
      boolean ok =
          (c >= 'A' && c <= 'Z')
              || (c >= 'a' && c <= 'z')
              || (c >= '0' && c <= '9')
              || c == '.'
              || c == '_'
              || c == '-';
      if (!ok) {
        return false;
      }
    }
    return true;
  }

  private static String json(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
