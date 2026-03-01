package org.tesis.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class ConfigLoader {
    private ConfigLoader() {
    }

    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] pieces = arg.substring(2).split("=", 2);
                String key = pieces[0];
                String value = pieces.length == 2 ? pieces[1] : "true";
                map.put(key, value);
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
