package com.example.migrator;

public class DBConfig {
    private String url = "jdbc:oracle:thin:@//localhost:1521/XEPDB1";
    private String username = "user";
    private String password = "pass";
    private String schema = "USER";

    public String getJdbcUrl() { return url; }
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getUsername() { return username; }
    public String getUser() { return username; }
    public void setUser(String u) { this.username = u; }
    public void setUsername(String u) { this.username = u; }

    public String getPassword() { return password; }
    public void setPassword(String p) { this.password = p; }

    public String getSchema() { return schema; }
    public void setSchema(String s) { this.schema = s; }
}
