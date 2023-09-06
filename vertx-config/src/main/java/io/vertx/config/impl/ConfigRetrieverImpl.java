/*
 * Copyright (c) 2014 Red Hat, Inc. and others
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package io.vertx.config.impl;


import io.vertx.config.ConfigChange;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.config.spi.ConfigProcessor;
import io.vertx.config.spi.ConfigStore;
import io.vertx.config.spi.ConfigStoreFactory;
import io.vertx.config.spi.utils.Processors;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * Implementation of the {@link ConfigRetriever}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ConfigRetrieverImpl implements ConfigRetriever {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRetrieverImpl.class);

  private static final String DEFAULT_CONFIG_PATH = "conf" + File.separator + "config.json";

  private final ContextInternal context;
  private final List<ConfigurationProvider> providers;
  private long scan;
  private final List<Handler<ConfigChange>> listeners = new ArrayList<>();
  private final ConfigStreamImpl streamOfConfiguration = new ConfigStreamImpl();
  private final ConfigRetrieverOptions options;

  private JsonObject current = new JsonObject();

  private Handler<Void> beforeScan;
  private Function<JsonObject, JsonObject> processor;

  public ConfigRetrieverImpl(Vertx vertx, ConfigRetrieverOptions options) {
    this.context = (ContextInternal) vertx.getOrCreateContext();
    this.options = options;

    ServiceLoader<ConfigStoreFactory> storeImpl =
        ServiceLoader.load(ConfigStoreFactory.class,
            ConfigStoreFactory.class.getClassLoader());

    Map<String, ConfigStoreFactory> nameToImplMap = new HashMap<>();
    storeImpl.iterator().forEachRemaining(factory -> nameToImplMap.put(factory.name(), factory));
    if (nameToImplMap.isEmpty()) {
      throw new IllegalStateException("No configuration store implementations found on the classpath");
    }

    List<ConfigStoreOptions> stores = options.getStores();
    if (options.isIncludeDefaultStores()) {
      stores = new ArrayList<>();
      stores.add(
        new ConfigStoreOptions().setType("json")
          .setConfig(vertx.getOrCreateContext().config()));
      stores.add(new ConfigStoreOptions().setType("sys"));
      stores.add(new ConfigStoreOptions().setType("env"));

      // Insert the default config if configured.
      String defaultConfigPath = getDefaultConfigPath();
      if (defaultConfigPath != null  && ! defaultConfigPath.trim().isEmpty()) {
        String format = extractFormatFromFileExtension(defaultConfigPath);
        LOGGER.info("Config file path: " + defaultConfigPath + ", format:" + format);
        stores.add(new ConfigStoreOptions()
          .setType("file").setFormat(format)
          .setOptional(true)
          .setConfig(new JsonObject().put("path", defaultConfigPath)));
      }
      stores.addAll(options.getStores());
    }

    // Iterate over the configured `stores` to configuration the stores
    providers = new ArrayList<>();
    for (ConfigStoreOptions option : stores) {
      String type = option.getType();
      if (type == null) {
        throw new IllegalArgumentException(
            "the `type` entry is mandatory in a configuration store configuration");
      }

      ConfigStoreFactory factory = nameToImplMap.get(type);
      if (factory == null) {
        throw new IllegalArgumentException("unknown configuration store implementation: " +
            type + " (known implementations are: " + nameToImplMap.keySet() + ")");
      }

      JsonObject config = option.getConfig();
      if (config == null) {
        config = new JsonObject();
      }
      ConfigStore store = factory.create(vertx, config);

      String format = option.getFormat() != null ? option.getFormat() : "json";
      ConfigProcessor processor = Processors.get(format);
      if (processor == null) {
        throw new IllegalArgumentException("unknown configuration format: " + format + " (supported formats are: " +
            Processors.getSupportedFormats());
      }
      providers.add(new ConfigurationProvider(store, processor, option.getConfig(), option.isOptional()));
    }
  }

  static String extractFormatFromFileExtension(String path) {
    int index = path.lastIndexOf(".");
    if (index == -1) {
      // Default format
      return "json";
    } else {
      String ext = path.substring(index + 1);
      if (ext.trim().isEmpty()) {
        return "json";
      }

      if ("yml".equalsIgnoreCase(ext)) {
        ext = "yaml";
      }
      return ext.toLowerCase();
    }
  }

  private String getDefaultConfigPath() {
    String value = System.getenv("VERTX_CONFIG_PATH");
    if (value == null  || value.trim().isEmpty()) {
      value = System.getProperty("vertx-config-path");
    }
    if (value != null  && ! value.trim().isEmpty()) {
      return value.trim();
    }
    File file = context.owner().resolveFile(DEFAULT_CONFIG_PATH);
    boolean exists = file != null && file.exists();
    if (exists) {
      return file.getAbsolutePath();
    }
    return null;
  }

  public synchronized void initializePeriodicScan() {
    if (options.getScanPeriod() > 0) {
      this.scan = context.setPeriodic(options.getScanPeriod(), l -> scan());
    } else {
      this.scan = -1;
    }
  }

  @Override
  public void getConfig(Handler<AsyncResult<JsonObject>> completionHandler) {
    Objects.requireNonNull(completionHandler);
    getConfig().onComplete(completionHandler);
  }

  @Override
  public Future<JsonObject> getConfig() {
    return compute().onSuccess(result -> {
      synchronized (this) {
        current = result;
      }
      streamOfConfiguration.handle(result);
    });
  }

  @Override
  public synchronized void close() {
    if (scan != -1) {
      context.owner().cancelTimer(scan);
    }

    streamOfConfiguration.close();

    for (ConfigurationProvider provider : providers) {
      provider.close();
    }
  }

  @Override
  public synchronized JsonObject getCachedConfig() {
    return current.copy();
  }

  @Override
  public synchronized void listen(Handler<ConfigChange> listener) {
    Objects.requireNonNull(listener);
    listeners.add(listener);
  }

  @Override
  public synchronized ConfigRetriever setBeforeScanHandler(Handler<Void> handler) {
    this.beforeScan = Objects.requireNonNull(handler, "The handler must not be `null`");
    return this;
  }

  @Override
  public synchronized ConfigRetriever setConfigurationProcessor(Function<JsonObject, JsonObject> processor) {
    this.processor = Objects.requireNonNull(processor, "The processor must not be `null`");
    return this;
  }

  @Override
  public ReadStream<JsonObject> configStream() {
    return streamOfConfiguration;
  }

  private void scan() {
    Handler<Void> h;
    synchronized (this) {
      h = this.beforeScan;
    }
    if (h != null) {
      h.handle(null);
    }
    compute().onFailure(throwable -> {
      streamOfConfiguration.fail(throwable);
      LOGGER.error("Error while scanning configuration", throwable);
    }).onSuccess(result -> {
      JsonObject prev;
      List<Handler<ConfigChange>> handlers;
      synchronized (this) {
        // Check for changes
        if (!current.equals(result)) {
          prev = current;
          current = result;
          handlers = !listeners.isEmpty() ? new ArrayList<>(listeners) : Collections.emptyList();
        } else {
          prev = null;
          handlers = null;
        }
      }
      if (handlers != null) {
        handlers.forEach(changeHandler -> changeHandler.handle(new ConfigChange(prev, result)));
        streamOfConfiguration.handle(result);
      }
    });
  }

  private Future<JsonObject> compute() {
    List<Future> futures = providers.stream()
      .map(s -> s.get(context.owner()))
      .collect(Collectors.toList());

    return CompositeFuture.all(futures).map(compositeFuture -> {
      // Merge the different futures
      JsonObject json = new JsonObject();
      futures.forEach(future -> json.mergeIn((JsonObject) future.result(), true));
      return json;
    }).map(json -> processor != null ? processor.apply(json) : json);
  }

  /**
   * @return the list of providers. For introspection purpose.
   */
  public List<ConfigurationProvider> getProviders() {
      return Collections.unmodifiableList(providers);
  }

  private class ConfigStreamImpl implements ReadStream<JsonObject> {

    private Handler<JsonObject> handler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> endHandler;

    private JsonObject last;
    private long demand = Long.MAX_VALUE;

    @Override
    public synchronized ReadStream<JsonObject> exceptionHandler(Handler<Throwable> handler) {
      Objects.requireNonNull(handler);
      this.exceptionHandler = handler;
      return this;
    }

    @Override
    public ReadStream<JsonObject> handler(Handler<JsonObject> handler) {
      Objects.requireNonNull(handler);
      JsonObject conf;
      synchronized (this) {
        this.handler = handler;
        conf = getCachedConfig();
      }

      if (conf != null && !conf.isEmpty()) {
        context.runOnContext(v -> this.handler.handle(conf));
      }

      return this;
    }

    @Override
    public synchronized ReadStream<JsonObject> pause() {
      demand = 0L;
      return this;
    }

    @Override
    public synchronized ReadStream<JsonObject> resume() {
      boolean check = demand == 0;
      demand = Long.MAX_VALUE;
      if (check) {
        checkPending();
      }
      return this;
    }

    @Override
    public synchronized ReadStream<JsonObject> fetch(long amount) {
      boolean check = demand == 0;
      demand += amount;
      if (demand < 0L) {
        demand = Long.MAX_VALUE;
      }
      if (check) {
        checkPending();
      }
      return this;
    }

    private void checkPending() {
      Handler<JsonObject> succ = handler;
      JsonObject conf = last;
      last = null;
      if (conf != null) {
        if (demand != Long.MAX_VALUE) {
          demand--;
        }
        if (succ != null) {
          context.runOnContext(v -> succ.handle(conf));
        }
      }
    }

    @Override
    public synchronized ReadStream<JsonObject> endHandler(Handler<Void> endHandler) {
      Objects.requireNonNull(endHandler);
      this.endHandler = endHandler;
      return this;
    }

    synchronized void handle(JsonObject conf) {
      Handler<JsonObject> succ = handler;
      boolean isPaused = demand == 0;
      if (isPaused) {
        last = conf;
      } else if (demand < Long.MAX_VALUE) {
        demand--;
      }

      if (!isPaused && succ != null) {
        context.runOnContext(v -> succ.handle(conf));
      }

    }

    void fail(Throwable cause) {
      Handler<Throwable> err;
      synchronized (this) {
        err = exceptionHandler;
      }

      if (err != null) {
        context.runOnContext(v -> err.handle(cause));
      }

    }

    void close() {
      Handler<Void> handler;
      synchronized (this) {
        handler = endHandler;
      }
      if (handler != null) {
        context.runOnContext(v -> handler.handle(null));
      }
    }
  }

  public Future<JsonObject> load(Vertx vertx, String format, List<String> args, List<String> profiles) {
    List<String> configs = getResourceFiles(filename ->
      filename.startsWith("config") && filename.endsWith("." + format) && !filename.equals("config." + format)
    );
    List<String> reqProfiles = new ArrayList<>();
    return loadConfigs(vertx, "config." + format, configs.toArray(new String[0])).map(loadedConfigs -> {
      JsonObject config = setProfile(loadedConfigs, JsonObject.of(), new String[]{"default"});
      if (profiles != null && !profiles.isEmpty()) {
        reqProfiles.addAll(profiles);
        setProfile(loadedConfigs, config, profiles.toArray(new String[0]));
      }
      if (args != null && !args.isEmpty()) {
        Optional<String[]> aProfiles = args.stream().filter(arg -> arg.toLowerCase().startsWith("--profiles=") || arg.toLowerCase().startsWith("-p=")).findFirst()
          .map(p -> (p.startsWith("-p=") ? p.substring(3) : p.substring("--profiles=".length())).split(","));
          aProfiles.ifPresent(strings -> {
            Collections.addAll(reqProfiles, strings);
            setProfile(loadedConfigs, config, strings);
          });
      }
      String env = System.getenv("VERTX_PROFILES");
      if (env != null) {
        String[] envProfiles = env.split(",");
        Collections.addAll(reqProfiles, envProfiles);
        setProfile(loadedConfigs, config, envProfiles);
      }
      return config;
    }).andThen(result -> {
      if (result.succeeded()) {
        setTimeDefaults(result.result());
        LOGGER.info("active profiles: " + String.join(",", reqProfiles));
      } else {
        LOGGER.error("load config failed cause: " + result.cause().getMessage(), result.cause());
      }
    });
  }

  private void setTimeDefaults(JsonObject config) {
    String envPrefix = config.getString("env-prefix", "").toUpperCase();
    String env = System.getenv(envPrefix + "_LOCATION");
    String location = env != null ? env : config.getString("location", "Asia/Jakarta");
    TimeZone.setDefault(TimeZone.getTimeZone(location));
    Locale.setDefault(Locale.forLanguageTag(location));
  }

  private JsonObject setProfile(JsonObject loadedConfigs, JsonObject defaultConfig, String[] profiles) {
    for (String profile : profiles) {
      JsonObject value = loadedConfigs.getJsonObject(profile);
      if (value == null) {
        LOGGER.warn("profile " + profile + " not found!");
        continue;
      }
      copySet(value, defaultConfig);
    }
    return defaultConfig;
  }

  private List<String> getResourceFiles(Predicate<String> filter) {
    ProtectionDomain protectionDomain = ConfigRetriever.class.getProtectionDomain();
    CodeSource codeSource = protectionDomain.getCodeSource();

    List<String> filenames = new ArrayList<>();
    if (codeSource != null) {
      String executablePath = codeSource.getLocation().getPath();

      if (executablePath.endsWith(".jar")) {
        try {
          JarFile jarFile = new JarFile(executablePath);
          Enumeration<JarEntry> entries = jarFile.entries();

          while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String entryName = entry.getName();
            if (filter.test(entryName)) {
              filenames.add(entryName);
            }
          }

          jarFile.close();
          return filenames;
        } catch (IOException e) {
          LOGGER.error("failed read file jar!", e);
        }
      } else {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL url = loader.getResource("");
        if (url == null) {
          throw new RuntimeException("resource is null");
        }
        String path = url.getPath();
        File[] files = Objects.requireNonNull(new File(path).listFiles());
        for (File file : files) {
          if (filter.test(file.getName())) {
            filenames.add(file.getName());
          }
        }
      }

    }
    LOGGER.error("Resource files is missing!");
    return filenames;
  }


  private Future<JsonObject> loadConfigs(Vertx vertx, String defaultConfig, String... configs) {
    List<Future<JsonObject>> futures = new ArrayList<>();
    futures.add(loadFileConfig(vertx, defaultConfig));
    for (String config : configs) {
      futures.add(loadFileConfig(vertx, config));
    }
    return Future.all(futures).map(compositeFuture -> {
      JsonObject loadedConfigs = JsonObject.of();
      for (int i = 0; i < compositeFuture.size(); i++) {
        JsonObject config = compositeFuture.resultAt(i);
        if (i == 0) {
          loadedConfigs.put("default", config);
          continue;
        }
        String profile = config.getString("profile");
        if (profile == null) {
          final String filename = FilenameUtils.getName(configs[i - 1]);
          profile = filename.substring(7, filename.length() - 4);
        }
        config.put("profile", profile);
        loadedConfigs.put(profile, config);
      }
      return loadedConfigs;
    });
  }

  private Future<JsonObject> loadFileConfig(Vertx vertx, String filePath) {
    File file = new File(filePath);
    ConfigStoreOptions configStoreOpt = new ConfigStoreOptions()
      .setType("file").setFormat(FilenameUtils.getExtension(filePath))
      .setConfig(io.vertx.core.json.JsonObject.of("path", file.getPath()));
    return ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(configStoreOpt)).getConfig();
  }

  private void copySet(JsonObject from, JsonObject to) {
    for (String fieldName : from.fieldNames()) {
      if (from.getValue(fieldName) instanceof JsonObject && to.getValue(fieldName) instanceof JsonObject) {
        copySet((JsonObject) from.getValue(fieldName), (JsonObject) to.getValue(fieldName));
      } else {
        to.put(fieldName, from.getValue(fieldName));
      }
    }
  }

}
