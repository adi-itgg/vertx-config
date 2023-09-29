package com.aitech.llc.yml;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.vertx.core.json.impl.JsonUtil.*;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;

@SuppressWarnings({"rawtypes", "unchecked"})
public class YmlJsonObject {

  private final JsonObject jsonObject;

  public YmlJsonObject(JsonObject jsonObject) {
    this.jsonObject = jsonObject;
  }

  public static YmlJsonObject of(JsonObject config) {
    return new YmlJsonObject(config);
  }

  private YmlJsonObject setYmlValue(String path, Object value) {
    return setYmlValue(path, value, false);
  }
  private YmlJsonObject setYmlValue(String path, Object value, boolean isRemove) {
    String[] sp = path.split("\\.");
    if (sp.length == 0) {
      if (isRemove) {
        jsonObject.remove(path);
      } else {
        jsonObject.put(path, value);
      }
      return this;
    }
    JsonObject temp = jsonObject;
    for (int i = 0; i < sp.length; i++) {
      String key = sp[i];
      if ((i + 1) == sp.length) {
        if (isRemove) {
          temp.remove(key);
        } else {
          temp.put(key, value);
        }
        return this;
      }
      JsonObject vo = temp.getJsonObject(key);
      if (vo == null) {
        vo = JsonObject.of();
      }
      temp.put(key, vo);
      temp = vo;
    }
    return this;
  }

  private <T> T getYmlValue(String path) {
    return getYmlValue(path, null);
  }
  @SuppressWarnings("unchecked")
  private <T> T getYmlValue(String path, T defaultValue) {
    String[] sp = path.split("\\.");
    if (sp.length == 0) {
      return (T) jsonObject.getValue(path);
    }
    JsonObject temp = jsonObject;
    for (int i = 0; i < sp.length; i++) {
      String key = sp[i];
      if ((i + 1) == sp.length) {
        return (T) temp.getValue(key);
      }
      Object v = temp.getValue(key);
      if (v instanceof JsonObject) {
        temp = (JsonObject) v;
      }
    }
    return defaultValue;
  }

  public String getString(String key) {
    return getString(key, null);
  }
  public String getString(String key, String def) {
    Object val = getYmlValue(key);
    if (val == null) {
      return def;
    }

    if (val instanceof Instant) {
      return ISO_INSTANT.format((Instant) val);
    } else if (val instanceof byte[]) {
      return BASE64_ENCODER.encodeToString((byte[]) val);
    } else if (val instanceof Buffer) {
      return BASE64_ENCODER.encodeToString(((Buffer) val).getBytes());
    } else if (val instanceof Enum) {
      return ((Enum) val).name();
    } else {
      return val.toString();
    }
  }

  public Number getNumber(String key) {
    return getNumber(key, null);
  }
  public Number getNumber(String key, Number def) {
    return getYmlValue(key, def);
  }

  public Integer getInteger(String key) {
    return getInteger(key, null);
  }
  public Integer getInteger(String key, Integer def) {
    Number number = getYmlValue(key);
    if (number == null) {
      return def;
    } else if (number instanceof Integer) {
      return (Integer) number;  // Avoids unnecessary unbox/box
    } else {
      return number.intValue();
    }
  }


  public Long getLong(String key) {
    return getLong(key, null);
  }
  public Long getLong(String key, Long def) {
    Number number = getYmlValue(key);
    if (number == null) {
      return def;
    } else if (number instanceof Long) {
      return (Long) number;  // Avoids unnecessary unbox/box
    } else {
      return number.longValue();
    }
  }


  public Double getDouble(String key) {
    return getDouble(key, null);
  }
  public Double getDouble(String key, Double def) {
    Number number = getYmlValue(key);
    if (number == null) {
      return def;
    } else if (number instanceof Double) {
      return (Double) number;  // Avoids unnecessary unbox/box
    } else {
      return number.doubleValue();
    }
  }


  public Float getFloat(String key) {
    return getFloat(key, null);
  }
  public Float getFloat(String key, Float def) {
    Number number = getYmlValue(key);
    if (number == null) {
      return null;
    } else if (number instanceof Float) {
      return (Float) number;  // Avoids unnecessary unbox/box
    } else {
      return number.floatValue();
    }
  }


  public Boolean getBoolean(String key) {
    return getBoolean(key, null);
  }
  public Boolean getBoolean(String key, Boolean def) {
    return getYmlValue(key, def);
  }


  public YmlJsonObject getYmlJsonObject(String key) {
    return YmlJsonObject.of(getJsonObject(key));
  }
  public YmlJsonObject getYmlJsonObject(String key, YmlJsonObject def) {
    return YmlJsonObject.of(getJsonObject(key, def.jsonObject));
  }
  public JsonObject getJsonObject(String key) {
    return getJsonObject(key, null);
  }
  public JsonObject getJsonObject(String key, JsonObject def) {
    Object val = getYmlValue(key);
    if (val == null) {
      return def;
    }
    if (val instanceof Map) {
      val = new JsonObject((Map) val);
    }
    return (JsonObject) val;
  }


  public JsonArray getJsonArray(String key) {
    return getJsonArray(key, null);
  }
  public JsonArray getJsonArray(String key, JsonArray def) {
    Object val = getYmlValue(key);
    if (val == null) {
      return def;
    }
    if (val instanceof List) {
      val = new JsonArray((List) val);
    }
    return (JsonArray) val;
  }


  public byte[] getBinary(String key) {
    return getBinary(key, null);
  }
  public byte[] getBinary(String key, byte[] def) {
    Object val = getYmlValue(key);
    // no-op
    if (val == null) {
      return def;
    }
    // no-op if value is already an byte[]
    if (val instanceof byte[]) {
      return (byte[]) val;
    }
    // unwrap if value is already a Buffer
    if (val instanceof Buffer) {
      return ((Buffer) val).getBytes();
    }
    // assume that the value is in String format as per RFC
    String encoded = (String) val;
    // parse to proper type
    return BASE64_DECODER.decode(encoded);
  }


  public Buffer getBuffer(String key) {
    return getBuffer(key, null);
  }
  public Buffer getBuffer(String key, Buffer def) {
    Object val = getYmlValue(key);
    // no-op
    if (val == null) {
      return def;
    }
    // no-op if value is already an Buffer
    if (val instanceof Buffer) {
      return (Buffer) val;
    }

    // wrap if value is already an byte[]
    if (val instanceof byte[]) {
      return Buffer.buffer((byte[]) val);
    }

    // assume that the value is in String format as per RFC
    String encoded = (String) val;
    // parse to proper type
    return Buffer.buffer(BASE64_DECODER.decode(encoded));
  }


  public Instant getInstant(String key) {
    return getInstant(key, null);
  }
  public Instant getInstant(String key, Instant def) {
    Object val = getYmlValue(key);
    // no-op
    if (val == null) {
      return def;
    }
    // no-op if value is already an Instant
    if (val instanceof Instant) {
      return (Instant) val;
    }
    // assume that the value is in String format as per RFC
    String encoded = (String) val;
    // parse to proper type
    return Instant.from(ISO_INSTANT.parse(encoded));
  }

  public Object getValue(String key) {
    return getValue(key, null);
  }
  public Object getValue(String key, Object def) {
    Object val = getYmlValue(key);
    if (val == null) {
      return def;
    }
    return wrapJsonValue(val);
  }


  public YmlJsonObject putNull(String key) {
    return setYmlValue(key, null);
  }


  public YmlJsonObject put(String key, Object value) {
    return setYmlValue(key, value);
  }

  public Object remove(String key) {
    return setYmlValue(key, null, true);
  }

}
