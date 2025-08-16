package com.example.migrator;

import java.io.*;
import java.nio.file.*;
import java.util.Base64;
import java.util.Properties;

public class ConfigStore {
    private static Path cfgFile() {
        String home = System.getProperty("user.home");
        Path dir = Paths.get(home, ".oracle-migrator");
        try { Files.createDirectories(dir); } catch (IOException ignored) {}
        return dir.resolve("db.properties");
    }

    private static String enc(String s) {
        if (s == null) return "";
        return Base64.getEncoder().encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    private static String dec(String s) {
        if (s == null || s.isBlank()) return "";
        try { return new String(Base64.getDecoder().decode(s), java.nio.charset.StandardCharsets.UTF_8); }
        catch (Exception e) { return s; }
    }

    public static void save(DBConfig src, DBConfig dst) throws IOException {
        Properties p = new Properties();
        p.setProperty("src.url", src.getUrl());
        p.setProperty("src.user", src.getUser());
        p.setProperty("src.pass", enc(src.getPassword()));
        p.setProperty("src.schema", src.getSchema());
        p.setProperty("dst.url", dst.getUrl());
        p.setProperty("dst.user", dst.getUser());
        p.setProperty("dst.pass", enc(dst.getPassword()));
        p.setProperty("dst.schema", dst.getSchema());
        try (OutputStream os = Files.newOutputStream(cfgFile())) {
            p.store(os, "Oracle Migrator Config");
        }
    }

    public static void loadInto(DBConfig src, DBConfig dst) throws IOException {
        Path f = cfgFile();
        if (!Files.exists(f)) return;
        Properties p = new Properties();
        try (InputStream is = Files.newInputStream(f)) {
            p.load(is);
        }
        src.setUrl(p.getProperty("src.url", src.getUrl()));
        src.setUser(p.getProperty("src.user", src.getUser()));
        src.setPassword(dec(p.getProperty("src.pass", "")));
        src.setSchema(p.getProperty("src.schema", src.getSchema()));

        dst.setUrl(p.getProperty("dst.url", dst.getUrl()));
        dst.setUser(p.getProperty("dst.user", dst.getUser()));
        dst.setPassword(dec(p.getProperty("dst.pass", "")));
        dst.setSchema(p.getProperty("dst.schema", dst.getSchema()));
    }
}
