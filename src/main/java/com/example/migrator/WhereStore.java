package com.example.migrator;

import java.io.IOException;
import java.nio.file.*;

public class WhereStore {
    private static Path baseDir() {
        Path dir = Paths.get(System.getProperty("user.home"), ".oracle-migrator", "where");
        try { Files.createDirectories(dir); } catch (IOException ignored) {}
        return dir;
    }
    private static String key(String schema, String table) {
        return (schema==null?"":schema.toUpperCase()) + "__" + (table==null?"":table.toUpperCase());
    }
    public static void saveWhere(String schema, String table, String where) throws IOException {
        Path f = baseDir().resolve(key(schema, table) + ".sql");
        Files.writeString(f, where==null?"":where, java.nio.charset.StandardCharsets.UTF_8);
    }
    public static String loadWhere(String schema, String table) {
        try {
            Path f = baseDir().resolve(key(schema, table) + ".sql");
            if (Files.exists(f)) return Files.readString(f);
        } catch (IOException ignored) {}
        return null;
    }
    public static boolean hasWhere(String schema, String table) {
        Path f = baseDir().resolve(key(schema, table) + ".sql");
        return Files.exists(f);
    }
}
