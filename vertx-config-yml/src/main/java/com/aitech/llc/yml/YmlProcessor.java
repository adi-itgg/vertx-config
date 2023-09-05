package com.aitech.llc.yml;

import io.vertx.config.spi.ConfigProcessor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

public class YmlProcessor implements ConfigProcessor {

  private static final LoaderOptions DEFAULT_OPTIONS = new LoaderOptions();

  @Override
  public String name() {
    return "yml";
  }

  @Override
  public Future<JsonObject> process(Vertx vertx, JsonObject configuration, Buffer input) {
    if (input.length() == 0) {
      // the parser does not support empty files, which should be managed to be homogeneous
      return ((ContextInternal) vertx.getOrCreateContext()).succeededFuture(new JsonObject());
    }

    // Use executeBlocking even if the bytes are in memory
    return vertx.executeBlocking(() -> {
      try {
        final Yaml yamlMapper = new Yaml(new SafeConstructor(DEFAULT_OPTIONS));
        Map<Object, Object> doc = yamlMapper.load(input.toString(StandardCharsets.UTF_8));
        return jsonify((String) doc.getOrDefault("env-prefix", ""), doc);
      } catch (ClassCastException e) {
        throw new DecodeException("Failed to decode YAML", e);
      }
    });
  }

  /**
   * Yaml allows map keys of type object, however json always requires key as String,
   * this helper method will ensure we adapt keys to the right type
   *
   * @param yaml yaml map
   * @return json map
   */
  @SuppressWarnings("unchecked")
  private static JsonObject jsonify(String path, Map<Object, Object> yaml) {
    if (yaml == null) {
      return null;
    }

    final JsonObject json = new JsonObject();

    for (Map.Entry<Object, Object> kv : yaml.entrySet()) {
      final String env = (path + ((path.isEmpty() ? "" : "_") + kv.getKey().toString()).toUpperCase()).replace("-", "");
      Object value = kv.getValue();
      if (value instanceof Map) {
        value = jsonify(env, (Map<Object, Object>) value);
      }
      // snake yaml handles dates as java.util.Date, and JSON does Instant
      if (value instanceof Date) {
        value = ((Date) value).toInstant();
      }
      json.put(kv.getKey().toString(), value);

      Map<String, String> envs = System.getenv();
      if (envs.containsKey(env)) {
        String envValue = System.getenv(env);
        if (isNumeric(envValue)) {
          if (envValue.contains(".")) {
            json.put(kv.getKey().toString(), Double.parseDouble(envValue));
          } else {
            json.put(kv.getKey().toString(), Long.parseLong(envValue));
          }
        } else {
          json.put(kv.getKey().toString(), envValue);
        }
      }
    }

    return json;
  }

  private static boolean isEmpty(CharSequence cs) {
    return cs == null || cs.length() == 0;
  }

  private static boolean isNumeric(CharSequence cs) {
    if (isEmpty(cs)) {
      return false;
    } else {
      int sz = cs.length();

      for(int i = 0; i < sz; ++i) {
        if (!Character.isDigit(cs.charAt(i))) {
          return false;
        }
      }

      return true;
    }
  }


}
