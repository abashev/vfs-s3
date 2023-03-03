package com.github.vfss3.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.internal.collections.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.Files.exists;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
class EnvironmentConfiguration {
    private final Pattern ENV_PATTERN = Pattern.compile("export\\s([^=]+)\\s*=\\s*(.+)");

    private final String configFile;

    private final Logger log = LoggerFactory.getLogger(EnvironmentConfiguration.class);

    public EnvironmentConfiguration() {
        this(".envrc");
    }

    public EnvironmentConfiguration(String configFile) {
        this.configFile = configFile;
    }

    /**
     * Get environment configuration as map
     *
     * @return
     */
    private Map<String, String> toMap() {
        Path envFile = Paths.get(configFile);
        Map<String, String> result = new HashMap<>();

        if (!exists(envFile)) {
            log.info("No {} file for loading credentials", configFile);
        } else {
            //read file into stream, try-with-resources
            try (Stream<String> stream = Files.lines(envFile)) {

                result = stream.
                        map(s -> {
                            Matcher m = ENV_PATTERN.matcher(s);

                            if (m.matches()) {
                                return (Pair.of(m.group(1), m.group(2)));
                            } else {
                                return null;
                            }
                        }).
                        filter(p -> (p != null)).
                        collect(Collectors.toMap(Pair::first, Pair::second));
            } catch (IOException e) {
                log.warn("Not able to read file " + envFile, e);
            }
        }

        return result;
    }

    /**
     * Get only value for specified key
     *
     * @param key
     * @return
     */
    public Optional<String> get(String key) {
        String value = toMap().get(key);

        if (value == null) {
            // Check for environment
            value = System.getenv(key);
        }

        return Optional.ofNullable(value);
    }

    public void computeIfPresent(String key, Consumer<String> valueConsumer) {
        get(key).ifPresent(valueConsumer);
    }

    public void computeIfPresent(String key1, String key2, BiConsumer<String, String> valueConsumer) {
        Optional<String> val1 = get(key1);
        Optional<String> val2 = get(key2);

        if (val1.isPresent() && val2.isPresent()) {
            valueConsumer.accept(val1.get(), val2.get());
        }
    }
}
