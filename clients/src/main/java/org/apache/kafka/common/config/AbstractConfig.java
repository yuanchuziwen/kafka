/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A convenient base class for configurations to extend.
 * <p>
 * This class holds both the original configuration that was provided as well as the parsed
 */
public class AbstractConfig {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Configs for which values have been requested, used to detect unused configs.
     * This set must be concurrent modifiable and iterable. It will be modified
     * when directly accessed or as a result of RecordingMap access.
     *
     * 用于检测未使用的配置的已请求值。
     * 此集合必须是并发可修改和可迭代的。当直接访问或作为RecordingMap访问的结果时，它将被修改。
     */
    private final Set<String> used = ConcurrentHashMap.newKeySet();

    /*
    * the original values passed in by the user
    * 由用户传入的原始值
    */
    private final Map<String, ?> originals;

    /*
    * the parsed values
    * 解析后的值
    */
    private final Map<String, Object> values;

    private final ConfigDef definition;

    public static final String CONFIG_PROVIDERS_CONFIG = "config.providers";

    private static final String CONFIG_PROVIDERS_PARAM = ".param.";

    /**
     * Construct a configuration with a ConfigDef and the configuration properties, which can include properties
     * for zero or more {@link ConfigProvider} that will be used to resolve variables in configuration property
     * values.
     *
     * 使用 ConfigDef 和配置属性构造配置，这些配置属性可以包括零个或多个{@link ConfigProvider}的属性，这些属性将用于解析配置属性值中的变量。
     *
     * The originals is a name-value pair configuration properties and optional config provider configs. The
     * value of the configuration can be a variable as defined below or the actual value. This constructor will
     * first instantiate the ConfigProviders using the config provider configs, then it will find all the
     * variables in the values of the originals configurations, attempt to resolve the variables using the named
     * ConfigProviders, and then parse and validate the configurations.
     *
     * originals 是名称-值对配置属性和可选的配置提供者配置。
     * 配置的值可以是下面定义的变量或实际值。
     * 此构造函数将首先使用配置提供者配置实例化ConfigProviders，然后它将在originals配置的值中找到所有变量，尝试使用命名的ConfigProviders解析变量，然后解析和验证配置。
     *
     * ConfigProvider configs can be passed either as configs in the originals map or in the separate
     * configProviderProps map. If config providers properties are passed in the configProviderProps any config
     * provider properties in originals map will be ignored. If ConfigProvider properties are not provided, the
     * constructor will skip the variable substitution step and will simply validate and parse the supplied
     * configuration.
     *
     * ConfigProvider 配置可以作为 originals 映射中的配置或单独的configProviderProps映射中的配置传递。
     * 如果配置提供者属性在configProviderProps中传递，则originals映射中的任何配置提供者属性将被忽略。
     * 如果未提供ConfigProvider属性，构造函数将跳过变量替换步骤，只会验证和解析提供的配置。
     *
     * The "{@code config.providers}" configuration property and all configuration properties that begin with the
     * "{@code config.providers.}" prefix are reserved. The "{@code config.providers}" configuration property
     * specifies the names of the config providers, and properties that begin with the "{@code config.providers..}"
     * prefix correspond to the properties for that named provider. For example, the "{@code config.providers..class}"
     * property specifies the name of the {@link ConfigProvider} implementation class that should be used for
     * the provider.
     *
     * "{@code config.providers}"配置属性和所有以"{@code config.providers.}"前缀开头的配置属性是保留的。
     * "{@code config.providers}"配置属性指定配置提供者的名称，以"{@code config.providers..}"前缀开头的属性对应于该命名提供者的属性。
     * 例如，"{@code config.providers..class}"属性指定应为提供者使用的{@link ConfigProvider}实现类的名称。
     *
     * The keys for ConfigProvider configs in both originals and configProviderProps will start with the above
     * mentioned "{@code config.providers.}" prefix.
     *
     * originals 和 configProviderProps 中的 ConfigProvider 配置的键都将以上述的"{@code config.providers.}"前缀开头。
     *
     * Variables have the form "${providerName:[path:]key}", where "providerName" is the name of a ConfigProvider,
     * "path" is an optional string, and "key" is a required string. This variable is resolved by passing the "key"
     * and optional "path" to a ConfigProvider with the specified name, and the result from the ConfigProvider is
     * then used in place of the variable. Variables that cannot be resolved by the AbstractConfig constructor will
     * be left unchanged in the configuration.
     *
     * 变量的形式为"${providerName:[path:]key}"，其中"providerName"是 ConfigProvider 的名称，"path"是可选字符串，"key"是必需字符串。
     * 通过将"key"和可选的"path"传递给具有指定名称的 ConfigProvider 来解析此变量，
     * 然后使用 ConfigProvider 的结果代替变量。无法由 AbstractConfig 构造函数解析的变量将在配置中保持不变。
     *
     * @param definition the definition of the configurations; may not be null
     *                  配置的定义；不能为空
     * @param originals the configuration properties plus any optional config provider properties;
     *                  配置属性加上任何可选的配置提供者属性；
     * @param configProviderProps the map of properties of config providers which will be instantiated by
     *        the constructor to resolve any variables in {@code originals}; may be null or empty
     *                  配置提供者的属性映射，构造函数将实例化这些属性以解析{@code originals}中的任何变量；可以为空或为空
     * @param doLog whether the configurations should be logged
     *              是否应记录配置
     */
    @SuppressWarnings("unchecked")
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals,  Map<String, ?> configProviderProps, boolean doLog) {
        // 检查所有的键是否都是字符串
        for (Map.Entry<?, ?> entry : originals.entrySet())
            if (!(entry.getKey() instanceof String))
                throw new ConfigException(entry.getKey().toString(), entry.getValue(), "Key must be a string.");

        // 解析配置变量
        // 内部会返回一个 ResolvingMap，包含 resolvedOriginals 和 originals
        // 实际使用的时候都会使用 resolvedOriginals
        this.originals = resolveConfigVariables(configProviderProps, (Map<String, Object>) originals);
        // 根据转入的 ConfigDef 解析配置并生成值
        this.values = definition.parse(this.originals);
        // 处理解析后的配置，返回的 map 会被用于更新配置
        Map<String, Object> configUpdates = postProcessParsedConfig(Collections.unmodifiableMap(this.values));
        for (Map.Entry<String, Object> update : configUpdates.entrySet()) {
            this.values.put(update.getKey(), update.getValue());
        }
        // 再次解析配置
        definition.parse(this.values);
        this.definition = definition;
        // 如果需要，记录所有配置
        if (doLog)
            logAll();
    }

    /**
     * Construct a configuration with a ConfigDef and the configuration properties,
     * which can include properties for zero or more {@link ConfigProvider}
     * that will be used to resolve variables in configuration property values.
     *
     * @param definition the definition of the configurations; may not be null
     * @param originals the configuration properties plus any optional config provider properties; may not be null
     */
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals) {
        this(definition, originals, Collections.emptyMap(), true);
    }

    /**
     * Construct a configuration with a ConfigDef and the configuration properties,
     * which can include properties for zero or more {@link ConfigProvider}
     * that will be used to resolve variables in configuration property values.
     *
     * @param definition the definition of the configurations; may not be null
     * @param originals the configuration properties plus any optional config provider properties; may not be null
     * @param doLog whether the configurations should be logged
     */
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        this(definition, originals, Collections.emptyMap(), doLog);

    }

    /**
     * Called directly after user configs got parsed (and thus default values got set).
     * This allows to change default values for "secondary defaults" if required.
     *
     * @param parsedValues unmodifiable map of current configuration
     * @return a map of updates that should be applied to the configuration (will be validated to prevent bad updates)
     */
    protected Map<String, Object> postProcessParsedConfig(Map<String, Object> parsedValues) {
        return Collections.emptyMap();
    }

    protected Object get(String key) {
        if (!values.containsKey(key))
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        used.add(key);
        return values.get(key);
    }

    public void ignore(String key) {
        used.add(key);
    }

    public Short getShort(String key) {
        return (Short) get(key);
    }

    public Integer getInt(String key) {
        return (Integer) get(key);
    }

    public Long getLong(String key) {
        return (Long) get(key);
    }

    public Double getDouble(String key) {
        return (Double) get(key);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return (List<String>) get(key);
    }

    public Boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public ConfigDef.Type typeOf(String key) {
        ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
        if (configKey == null)
            return null;
        return configKey.type;
    }

    public String documentationOf(String key) {
        ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
        if (configKey == null)
            return null;
        return configKey.documentation;
    }

    public Password getPassword(String key) {
        return (Password) get(key);
    }

    public Class<?> getClass(String key) {
        return (Class<?>) get(key);
    }

    public Set<String> unused() {
        Set<String> keys = new HashSet<>(originals.keySet());
        keys.removeAll(used);
        return keys;
    }

    public Map<String, Object> originals() {
        Map<String, Object> copy = new RecordingMap<>();
        copy.putAll(originals);
        return copy;
    }

    public Map<String, Object> originals(Map<String, Object> configOverrides) {
        Map<String, Object> copy = new RecordingMap<>();
        copy.putAll(originals);
        copy.putAll(configOverrides);
        return copy;
    }

    /**
     * Get all the original settings, ensuring that all values are of type String.
     * @return the original settings
     * @throws ClassCastException if any of the values are not strings
     */
    public Map<String, String> originalsStrings() {
        Map<String, String> copy = new RecordingMap<>();
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (!(entry.getValue() instanceof String))
                throw new ClassCastException("Non-string value found in original settings for key " + entry.getKey() +
                        ": " + (entry.getValue() == null ? null : entry.getValue().getClass().getName()));
            copy.put(entry.getKey(), (String) entry.getValue());
        }
        return copy;
    }

    /**
     * Gets all original settings with the given prefix, stripping the prefix before adding it to the output.
     *
     * @param prefix the prefix to use as a filter
     * @return a Map containing the settings with the prefix
     */
    public Map<String, Object> originalsWithPrefix(String prefix) {
        return originalsWithPrefix(prefix, true);
    }

    /**
     * Gets all original settings with the given prefix.
     *
     * @param prefix the prefix to use as a filter
     * @param strip strip the prefix before adding to the output if set true
     * @return a Map containing the settings with the prefix
     */
    public Map<String, Object> originalsWithPrefix(String prefix, boolean strip) {
        Map<String, Object> result = new RecordingMap<>(prefix, false);
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                if (strip)
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                else
                    result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Put all keys that do not start with {@code prefix} and their parsed values in the result map and then
     * put all the remaining keys with the prefix stripped and their parsed values in the result map.
     *
     * This is useful if one wants to allow prefixed configs to override default ones.
     * <p>
     * Two forms of prefixes are supported:
     * <ul>
     *     <li>listener.name.{listenerName}.some.prop: If the provided prefix is `listener.name.{listenerName}.`,
     *         the key `some.prop` with the value parsed using the definition of `some.prop` is returned.</li>
     *     <li>listener.name.{listenerName}.{mechanism}.some.prop: If the provided prefix is `listener.name.{listenerName}.`,
     *         the key `{mechanism}.some.prop` with the value parsed using the definition of `some.prop` is returned.
     *          This is used to provide per-mechanism configs for a broker listener (e.g sasl.jaas.config)</li>
     * </ul>
     * </p>
     */
    public Map<String, Object> valuesWithPrefixOverride(String prefix) {
        Map<String, Object> result = new RecordingMap<>(values(), prefix, true);
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                String keyWithNoPrefix = entry.getKey().substring(prefix.length());
                ConfigDef.ConfigKey configKey = definition.configKeys().get(keyWithNoPrefix);
                if (configKey != null)
                    result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                else {
                    String keyWithNoSecondaryPrefix = keyWithNoPrefix.substring(keyWithNoPrefix.indexOf('.') + 1);
                    configKey = definition.configKeys().get(keyWithNoSecondaryPrefix);
                    if (configKey != null)
                        result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                }
            }
        }
        return result;
    }

    /**
     * If at least one key with {@code prefix} exists, all prefixed values will be parsed and put into map.
     * If no value with {@code prefix} exists all unprefixed values will be returned.
     *
     * This is useful if one wants to allow prefixed configs to override default ones, but wants to use either
     * only prefixed configs or only regular configs, but not mix them.
     */
    public Map<String, Object> valuesWithPrefixAllOrNothing(String prefix) {
        Map<String, Object> withPrefix = originalsWithPrefix(prefix, true);

        if (withPrefix.isEmpty()) {
            return new RecordingMap<>(values(), "", true);
        } else {
            Map<String, Object> result = new RecordingMap<>(prefix, true);

            for (Map.Entry<String, ?> entry : withPrefix.entrySet()) {
                ConfigDef.ConfigKey configKey = definition.configKeys().get(entry.getKey());
                if (configKey != null)
                    result.put(entry.getKey(), definition.parseValue(configKey, entry.getValue(), true));
            }

            return result;
        }
    }

    public Map<String, ?> values() {
        return new RecordingMap<>(values);
    }

    public Map<String, ?> nonInternalValues() {
        Map<String, Object> nonInternalConfigs = new RecordingMap<>();
        values.forEach((key, value) -> {
            ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
            if (configKey == null || !configKey.internalConfig) {
                nonInternalConfigs.put(key, value);
            }
        });
        return nonInternalConfigs;
    }

    private void logAll() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);

        for (Map.Entry<String, Object> entry : new TreeMap<>(this.values).entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }

    /**
     * Log warnings for any unused configurations
     */
    public void logUnused() {
        for (String key : unused())
            log.warn("The configuration '{}' was supplied but isn't a known config.", key);
    }

    private <T> T getConfiguredInstance(Object klass, Class<T> t, Map<String, Object> configPairs) {
        if (klass == null)
            return null;

        Object o;
        if (klass instanceof String) {
            try {
                o = Utils.newInstance((String) klass, t);
            } catch (ClassNotFoundException e) {
                throw new KafkaException("Class " + klass + " cannot be found", e);
            }
        } else if (klass instanceof Class<?>) {
            o = Utils.newInstance((Class<?>) klass);
        } else
            throw new KafkaException("Unexpected element of type " + klass.getClass().getName() + ", expected String or Class");
        if (!t.isInstance(o))
            throw new KafkaException(klass + " is not an instance of " + t.getName());
        if (o instanceof Configurable)
            ((Configurable) o).configure(configPairs);

        return t.cast(o);
    }

    /**
     * Get a configured instance of the give class specified by the given configuration key. If the object implements
     * Configurable configure it using the configuration.
     *
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @return A configured instance of the class
     */
    public <T> T getConfiguredInstance(String key, Class<T> t) {
        return getConfiguredInstance(key, t, Collections.emptyMap());
    }

    /**
     * Get a configured instance of the give class specified by the given configuration key. If the object implements
     * Configurable configure it using the configuration.
     *
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @param configOverrides override origin configs
     * @return A configured instance of the class
     */
    public <T> T getConfiguredInstance(String key, Class<T> t, Map<String, Object> configOverrides) {
        Class<?> c = getClass(key);

        return getConfiguredInstance(c, t, originals(configOverrides));
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
        return getConfiguredInstances(key, t, Collections.emptyMap());
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     * @param key The configuration key for the class
     * @param t The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
        return getConfiguredInstances(getList(key), t, configOverrides);
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     * @param classNames The list of class names of the instances to create
     * @param t The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(List<String> classNames, Class<T> t, Map<String, Object> configOverrides) {
        List<T> objects = new ArrayList<>();
        if (classNames == null)
            return objects;
        Map<String, Object> configPairs = originals();
        configPairs.putAll(configOverrides);
        for (Object klass : classNames) {
            Object o = getConfiguredInstance(klass, t, configPairs);
            objects.add(t.cast(o));
        }
        return objects;
    }

    /**
     * 从给定的 configMap 中提取潜在的变量配置。
     * 潜在变量是形式为 "${providerName:[path:]key}" 的元组。
     *
     * @param configMap 包含潜在变量的配置映射
     * @return 包含潜在变量的字符串映射
     */
    private Map<String, String> extractPotentialVariables(Map<?, ?>  configMap) {
        // 变量是形式为 "${providerName:[path:]key}" 的元组。从 configMap 中提取具有字符串值的子集作为潜在变量。
        Map<String, String> configMapAsString = new HashMap<>();
        // 遍历 configMap 的每个键值对
        for (Map.Entry<?, ?> entry : configMap.entrySet()) {
            // 如果值是字符串，则将其添加到 configMapAsString
            if (entry.getValue() instanceof String)
                configMapAsString.put((String) entry.getKey(), (String) entry.getValue());
        }

        return configMapAsString;
    }

    /**
     * Instantiates given list of config providers and fetches the actual values of config variables from the config providers.
     * returns a map of config key and resolved values.
     * 实例化给定的配置提供者列表，并从配置提供者那里获取实际的配置变量值。
     * 返回一个包含配置键和解析值的映射。
     *
     * @param configProviderProps The map of config provider configs
     * 配置提供者配置的映射
     * @param originals The map of raw configs.
     * 原始配置的映射
     * @return map of resolved config variable.
     * 解析的配置变量映射
     */
    @SuppressWarnings("unchecked")
    private  Map<String, ?> resolveConfigVariables(Map<String, ?> configProviderProps, Map<String, Object> originals) {
        // 用于存储潜在的变量配置的映射
        Map<String, String> providerConfigString;
        // 用于存储配置属性的映射
        Map<String, ?> configProperties;
        // 用于存储已解析的原始配置的映射
        Map<String, Object> resolvedOriginals = new HashMap<>();
        // 将原始配置中的变量提取为潜在的变量配置
        // 其实就是把 originals 中的 String 类型的 value 提取出来，然后放到 indirectVariables 中
        Map<String, String> indirectVariables = extractPotentialVariables(originals);

        resolvedOriginals.putAll(originals);
        // 如果 configProviderProps 为空或为空，则使用原始配置
        if (configProviderProps == null || configProviderProps.isEmpty()) {
            providerConfigString = indirectVariables;
            configProperties = originals;
        } else {
            // 提取 configProviderProps 中的潜在变量配置
            // 针对传入的 configProviderProps 中的 String 类型的 value 提取出来，然后放到 providerConfigString 中
            providerConfigString = extractPotentialVariables(configProviderProps);
            // 使用 configProviderProps 作为配置属性
            configProperties = configProviderProps;
        }
        // 实例化配置提供者
        Map<String, ConfigProvider> providers = instantiateConfigProviders(providerConfigString, configProperties);

        // 如果提供了配置提供者，则转换间接变量
        if (!providers.isEmpty()) {
            // 创建 ConfigTransformer 并转换间接变量
            ConfigTransformer configTransformer = new ConfigTransformer(providers);
            ConfigTransformerResult result = configTransformer.transform(indirectVariables);
            // 如果转换结果不为空，则将结果添加到 resolvedOriginals
            if (!result.data().isEmpty()) {
                resolvedOriginals.putAll(result.data());
            }
        }
        // 关闭所有配置提供者
        providers.values().forEach(x -> Utils.closeQuietly(x, "config provider"));

        // 返回 ResolvingMap，包含 resolvedOriginals 和 originals
        return new ResolvingMap<>(resolvedOriginals, originals);
    }

    private Map<String, Object> configProviderProperties(String configProviderPrefix, Map<String, ?> providerConfigProperties) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, ?> entry : providerConfigProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(configProviderPrefix) && key.length() > configProviderPrefix.length()) {
                result.put(key.substring(configProviderPrefix.length()), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Instantiates and configures the ConfigProviders. The config providers configs are defined as follows:
     * config.providers : A comma-separated list of names for providers.
     * config.providers.{name}.class : The Java class name for a provider.
     * config.providers.{name}.param.{param-name} : A parameter to be passed to the above Java class on initialization.
     * returns a map of config provider name and its instance.
     * @param indirectConfigs The map of potential variable configs
     * @param providerConfigProperties The map of config provider configs
     * @return map map of config provider name and its instance.
     */
    private Map<String, ConfigProvider> instantiateConfigProviders(Map<String, String> indirectConfigs, Map<String, ?> providerConfigProperties) {
        final String configProviders = indirectConfigs.get(CONFIG_PROVIDERS_CONFIG);

        if (configProviders == null || configProviders.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> providerMap = new HashMap<>();

        for (String provider: configProviders.split(",")) {
            String providerClass = CONFIG_PROVIDERS_CONFIG + "." + provider + ".class";
            if (indirectConfigs.containsKey(providerClass))
                providerMap.put(provider, indirectConfigs.get(providerClass));

        }
        // Instantiate Config Providers
        Map<String, ConfigProvider> configProviderInstances = new HashMap<>();
        for (Map.Entry<String, String> entry : providerMap.entrySet()) {
            try {
                String prefix = CONFIG_PROVIDERS_CONFIG + "." + entry.getKey() + CONFIG_PROVIDERS_PARAM;
                Map<String, ?> configProperties = configProviderProperties(prefix, providerConfigProperties);
                ConfigProvider provider = Utils.newInstance(entry.getValue(), ConfigProvider.class);
                provider.configure(configProperties);
                configProviderInstances.put(entry.getKey(), provider);
            } catch (ClassNotFoundException e) {
                log.error("ClassNotFoundException exception occurred: " + entry.getValue());
                throw new ConfigException("Invalid config:" + entry.getValue() + " ClassNotFoundException exception occurred", e);
            }
        }

        return configProviderInstances;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractConfig that = (AbstractConfig) o;

        return originals.equals(that.originals);
    }

    @Override
    public int hashCode() {
        return originals.hashCode();
    }

    /**
     * Marks keys retrieved via `get` as used. This is needed because `Configurable.configure` takes a `Map` instead
     * of an `AbstractConfig` and we can't change that without breaking public API like `Partitioner`.
     */
    private class RecordingMap<V> extends HashMap<String, V> {

        private final String prefix;
        private final boolean withIgnoreFallback;

        RecordingMap() {
            this("", false);
        }

        RecordingMap(String prefix, boolean withIgnoreFallback) {
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        RecordingMap(Map<String, ? extends V> m) {
            this(m, "", false);
        }

        RecordingMap(Map<String, ? extends V> m, String prefix, boolean withIgnoreFallback) {
            super(m);
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        @Override
        public V get(Object key) {
            if (key instanceof String) {
                String stringKey = (String) key;
                String keyWithPrefix;
                if (prefix.isEmpty()) {
                    keyWithPrefix = stringKey;
                } else {
                    keyWithPrefix = prefix + stringKey;
                }
                ignore(keyWithPrefix);
                if (withIgnoreFallback)
                    ignore(stringKey);
            }
            return super.get(key);
        }
    }

    /**
     * ResolvingMap keeps a track of the original map instance and the resolved configs.
     * The originals are tracked in a separate nested map and may be a `RecordingMap`; thus
     * any access to a value for a key needs to be recorded on the originals map.
     * The resolved configs are kept in the inherited map and are therefore mutable, though any
     * mutations are not applied to the originals.
     * ResolvingMap 会跟踪原始 map 实例和已解析的配置。
     * 原始配置在一个单独的嵌套 map 中进行跟踪，可能是 RecordingMap；因此，对键的任何值的访问都需要在原始 map 上记录。
     * 已解析的配置保存在继承的 map 中，因此是可变的，尽管任何更改都不会应用于原始配置。
     */
    private static class ResolvingMap<V> extends HashMap<String, V> {

        // 内部 map 用于存储原始配置
        private final Map<String, ?> originals;

        ResolvingMap(Map<String, ? extends V> resolved, Map<String, ?> originals) {
            super(resolved);
            this.originals = Collections.unmodifiableMap(originals);
        }

        @Override
        public V get(Object key) {
            if (key instanceof String && originals.containsKey(key)) {
                // Intentionally ignore the result; call just to mark the original entry as used
                // 故意忽略结果；只是为了标记原始条目已使用
                originals.get(key);
            }
            // But always use the resolved entry
            // 但总是使用已解析的条目
            return super.get(key);
        }
    }
}
