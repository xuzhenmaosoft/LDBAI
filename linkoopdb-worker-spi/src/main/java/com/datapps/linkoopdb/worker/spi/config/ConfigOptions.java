package com.datapps.linkoopdb.worker.spi.config;

/**
 * @author yukkit.zhang
 * @date 2020/8/20
 */
public class ConfigOptions {

    public static OptionBuilder key(String key) {
        return new OptionBuilder(key);
    }

    public static final class OptionBuilder {

        private final String key;

        OptionBuilder(String key) {
            this.key = key;
        }

        public <T> PropertyDetilBuilder<T> defaultValue(T value) {
            return new PropertyDetilBuilder<>(key, value);
        }

        public PropertyDetilBuilder<String> noDefaultValue() {
            return new PropertyDetilBuilder<>(key, "");
        }
    }

    private ConfigOptions() {}
}
