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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        return jsonify((String) doc.getOrDefault("prop-prefix", ""), (String) doc.getOrDefault("env-prefix", ""), doc);
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
  private static JsonObject jsonify(String propPath, String envPath, Map<Object, Object> yaml) {
    if (yaml == null) {
      return null;
    }

    final JsonObject json = new JsonObject();

    for (Map.Entry<Object, Object> kv : yaml.entrySet()) {
      final String prop = (propPath + ((propPath.isEmpty() ? "" : ".") + kv.getKey().toString()).toLowerCase()); // for best practice props require lowercase!
      final String env = (envPath + ((envPath.isEmpty() ? "" : "_") + kv.getKey().toString()).toUpperCase()).replace("-", "");
      Object value = kv.getValue();
      if (value instanceof Map) {
        value = jsonify(prop, env, (Map<Object, Object>) value);
      }
      // snake yaml handles dates as java.util.Date, and JSON does Instant
      if (value instanceof Date) {
        value = ((Date) value).toInstant();
      }
      json.put(kv.getKey().toString(), value);

      String envValue = System.getenv(env);
      if (envValue != null) {
        json.put(kv.getKey().toString(), parseData(envValue));
      }

      String propValue = System.getProperty(prop);
      if (propValue != null) {
        json.put(kv.getKey().toString(), parseData(propValue));
      }

    }

    return json;
  }

  private static Object parseData(String value) {
    if (value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
      return value.substring(1, value.length() - 1);
    }
    return tries(value,
      Long::parseLong,
      Double::parseDouble,
      Boolean::parseBoolean,
      (v) -> {
        if (v.charAt(0) == '[' && v.charAt(v.length() - 1) == ']') {
          String input = v.substring(1, v.length() - 1);
          Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
          Matcher matcher = pattern.matcher(input);
          List<Object> data = new ArrayList<>();
          int lastIndex = 0;
          while (matcher.find()) {
            String c = input.substring(lastIndex, matcher.start());
            lastIndex = matcher.end();
            data.add(parseData(c));
          }
          if (lastIndex < input.length() - 1) {
            String c = input.substring(lastIndex, input.length() - 1);
            data.add(parseData(c));
          }
          return data;
        }
        throw new IllegalArgumentException("can't parse as list");
      }
    );
  }

  @SafeVarargs
  private static Object tries(String value, Function<String, Object>...actions) {
    for (Function<String, Object> consumer : actions) {
      try {
        return consumer.apply(value);
      } catch (Throwable e) {
        // can't parse data
      }
    }
    return value;
  }

}
