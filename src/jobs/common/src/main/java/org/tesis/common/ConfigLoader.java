package org.tesis.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Parses CLI arguments in both {@code --key=value} and {@code --key value}
 * formats so that the same jobs can be driven by Spark (uses {@code =})
 * and by Flink's {@code flink run} (uses space-separated tokens).
 */
public final class ConfigLoader {
    private ConfigLoader() {
    }

    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                continue;
            }
            String token = args[i].substring(2); // strip leading "--"
            if (token.contains("=")) {
                // --key=value (Spark / Gradle style)
                String[] parts = token.split("=", 2);
                map.put(parts[0], parts[1]);
            } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                // --key value (Flink flink-run style)
                map.put(token, args[++i]);
            } else {
                // --flag (boolean flag with no value)
                map.put(token, "true");
            }
        }
        return map;
    }

    public static Properties jdbcProperties(String user, String password) {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("driver", "org.postgresql.Driver");
        properties.setProperty("stringtype", "unspecified");
        return properties;
    }
}
