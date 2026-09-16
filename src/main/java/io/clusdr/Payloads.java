package io.clusdr;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/** Encode a publish payload onto the wire. */
public final class Payloads {
  private Payloads() {}

  /**
   * Bytes on the wire for {@link Cluster#publish(String, Object)}.
   *
   * <ul>
   *   <li>{@code null} → empty
   *   <li>{@code byte[]} → as-is
   *   <li>{@link String} → UTF-8
   *   <li>{@link Map} with string keys → compact JSON object (string values only)
   * </ul>
   *
   * Anything else throws {@link IllegalArgumentException}.
   */
  public static byte[] encode(Object payload) {
    if (payload == null) {
      return new byte[0];
    }
    if (payload instanceof byte[] bytes) {
      return bytes;
    }
    if (payload instanceof String s) {
      return s.getBytes(StandardCharsets.UTF_8);
    }
    if (payload instanceof Map<?, ?> map) {
      return jsonObject(map).getBytes(StandardCharsets.UTF_8);
    }
    throw new IllegalArgumentException(
        "clusdr: payload must be bytes, string, or Map; got " + payload.getClass().getName());
  }

  private static String jsonObject(Map<?, ?> map) {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    boolean first = true;
    for (Map.Entry<?, ?> e : map.entrySet()) {
      if (!(e.getKey() instanceof String key)) {
        throw new IllegalArgumentException("clusdr: payload map keys must be strings");
      }
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append(jsonString(key)).append(':').append(jsonValue(e.getValue()));
    }
    sb.append('}');
    return sb.toString();
  }

  private static String jsonValue(Object v) {
    if (v == null) {
      return "null";
    }
    if (v instanceof String s) {
      return jsonString(s);
    }
    if (v instanceof Number || v instanceof Boolean) {
      return Objects.toString(v);
    }
    throw new IllegalArgumentException(
        "clusdr: payload map values must be string, number, boolean, or null; got "
            + v.getClass().getName());
  }

  private static String jsonString(String s) {
    StringBuilder sb = new StringBuilder(s.length() + 2);
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    sb.append('"');
    return sb.toString();
  }
}
