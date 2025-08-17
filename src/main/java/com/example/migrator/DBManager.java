package com.example.migrator;

import java.sql.*;
import java.util.*;

public class DBManager {

    public static Connection open(DBConfig cfg) throws SQLException {
        return DriverManager.getConnection(cfg.getJdbcUrl(), cfg.getUsername(), cfg.getPassword());
    }

    public static class StopSignal {
        private volatile boolean stopped;
        public void stop() { stopped = true; }
        public boolean isStopped() { return stopped; }
        public void reset() { stopped = false; }
    }

    public interface ProgressListener { void onBatch(long totalTransferred, double rowsPerSec); }
    public interface DeleteListener { void onBatch(long deletedSoFar); }

    public static List<String> listTables(Connection conn, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        String sql = "SELECT table_name FROM all_tables WHERE owner = ? ORDER BY table_name";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    public static boolean tableExists(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM all_tables WHERE owner = ? AND table_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next(); return rs.getInt(1) > 0;
            }
        }
    }


    public static final class CountAndSize {
        public final long rowCount;
        public final long totalBytes;
        public CountAndSize(long rowCount, long totalBytes) {
            this.rowCount = rowCount;
            this.totalBytes = totalBytes;
        }
    }


    public static CountAndSize getCountAndSizeFromSource(Connection conn,
                                                         String schema,
                                                         String table,
                                                         String whereClause) throws SQLException {
        String fq = schema + "." + table; // Annahme: normale (uppercased) Identifiers
        String whereSql = (whereClause != null && !whereClause.isBlank()) ? " WHERE " + whereClause : "";

        // Eine Abfrage, zwei Skalar-Subqueries:
        String sql =
                "SELECT " +
                        "  (SELECT COUNT(*) FROM " + fq + whereSql + ") AS row_count, " +
                        "  ( " +
                        "    WITH t AS (SELECT UPPER(?) AS table_name FROM dual), " +
                        "         idx AS (SELECT ui.index_name AS segment_name " +
                        "                 FROM user_indexes ui JOIN t ON ui.table_name = t.table_name), " +
                        "         lob AS ( " +
                        "           SELECT ul.segment_name FROM user_lobs ul JOIN t ON ul.table_name = t.table_name " +
                        "           UNION ALL " +
                        "           SELECT ul.index_name   FROM user_lobs ul JOIN t ON ul.table_name = t.table_name " +
                        "         ) " +
                        "    SELECT NVL(SUM(us.bytes),0) " +
                        "    FROM   user_segments us " +
                        "    WHERE  (us.segment_type = 'TABLE' AND us.segment_name = (SELECT table_name FROM t)) " +
                        "       OR  us.segment_name IN (SELECT segment_name FROM idx) " +
                        "       OR  us.segment_name IN (SELECT segment_name FROM lob) " +
                        "  ) AS total_size_bytes " +
                        "FROM dual";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table); // für UPPER(?) in CTE t
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long cnt   = rs.getLong(1);
                long bytes = rs.getLong(2);
                return new CountAndSize(cnt, bytes);
            }
        }
    }


    public static long countRows(Connection conn, String schema, String table, String whereClause) throws SQLException {
        String fq = schema + "." + table;
        String sql = "SELECT COUNT(*) FROM " + fq + (whereClause != null && !whereClause.isBlank() ? " WHERE " + whereClause : "");
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next(); return rs.getLong(1);
        }
    }

    public static void copyTable(Connection src, String srcSchema, Connection dst, String dstSchema, String table,
                                 String whereClause, int commitBatch, ProgressListener listener, StopSignal stop) throws SQLException {
        String fqSrc = srcSchema + "." + table;
        String fqDst = dstSchema + "." + table;
        src.setAutoCommit(false);
        dst.setAutoCommit(false);

        List<String> cols = new ArrayList<>();
        try (Statement st = src.createStatement(); ResultSet rs = st.executeQuery("SELECT * FROM " + fqSrc + " WHERE 1=0")) {
            var md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) cols.add(md.getColumnName(i));
        }
        String sel = "SELECT " + String.join(",", cols) + " FROM " + fqSrc + (whereClause!=null && !whereClause.isBlank() ? " WHERE " + whereClause : "");
        String placeholders = String.join(",", java.util.Collections.nCopies(cols.size(), "?"));
        String ins = "INSERT INTO " + fqDst + " (" + String.join(",", cols) + ") VALUES (" + placeholders + ")";

        long transferred = 0;
        long started = System.nanoTime();

        try (PreparedStatement pin = dst.prepareStatement(ins);
             Statement sst = src.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            sst.setFetchSize(1000);
            try (ResultSet rs = sst.executeQuery(sel)) {
                int batch = 0;
                while (rs.next()) {
                    if (stop != null && stop.isStopped()) { dst.rollback(); throw new SQLException("stopped"); }
                    for (int i = 0; i < cols.size(); i++) pin.setObject(i+1, rs.getObject(i+1));
                    pin.addBatch();
                    batch++; transferred++;
                    if (batch >= commitBatch) {
                        pin.executeBatch(); dst.commit(); batch = 0;
                        double sec = (System.nanoTime()-started)/1_000_000_000.0;
                        double rate = Math.round(sec>0 ? transferred/sec : 0);
                        if (listener != null) listener.onBatch(transferred, rate);
                    }
                }
                if (batch>0) { pin.executeBatch(); dst.commit(); }
            }
        }
    }

   // public static int deleteRowsInBatches(Connection conn, String schema, String table, String whereClause, int batchSize, DeleteListener listener) throws SQLException {
   public static int deleteRowsInBatches(Connection conn, String schema, String table, String whereClause, int batchSize, DeleteListener listener, StopSignal stop) throws SQLException {
        final String base = schema + "." + table;
        final String sql = (whereClause != null && !whereClause.isBlank())
                ? "DELETE FROM " + base + " WHERE (" + whereClause + ") AND ROWNUM <= ?"
                : "DELETE FROM " + base + " WHERE ROWNUM <= ?";

        int total = 0;
        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            while (true) {
                if (stop != null && stop.isStopped()) { conn.rollback(); throw new SQLException("stopped"); }
                ps.setInt(1, batchSize);
                int aff = ps.executeUpdate();
                conn.commit();
                total += aff;
                if (listener != null) listener.onBatch(total);
                if (aff < batchSize) break;
            }
        }
        return total;
    }

    // --- DDL generation: table + PK + indexes + FKs
    public static void createTableLikeSource(Connection src, String srcSchema, Connection dst, String dstSchema, String table) throws SQLException {
        if (tableExists(dst, dstSchema, table)) return;
        String ddl = buildCreateTableDDL(src, srcSchema, dstSchema, table);
        try (Statement st = dst.createStatement()) {
            st.executeUpdate(ddl);
            //    dst.commit();
        }
        // Add PK
        String pkDdl = buildAddPkDDL(src, srcSchema, dstSchema, table);
        if (pkDdl != null) {
            try (Statement st = dst.createStatement()) { st.executeUpdate(pkDdl); dst.commit(); } catch (SQLException ex) { /* ignore */ }
        }
        // Indexes
        for (String idxDdl : buildCreateIndexesDDL(src, srcSchema, dstSchema, table)) {
            try (Statement st = dst.createStatement()) { st.executeUpdate(idxDdl); dst.commit(); } catch (SQLException ex) { /* ignore */ }
        }
        // FKs
        for (String fkDdl : buildAddFksDDL(src, srcSchema, dstSchema, table)) {
            try (Statement st = dst.createStatement()) { st.executeUpdate(fkDdl); dst.commit(); } catch (SQLException ex) { /* ignore */ }
        }
    }

    private static String buildCreateTableDDL(Connection src, String srcSchema, String dstSchema, String table) throws SQLException {
        String sql = "SELECT column_name, data_type, data_length, char_used, data_precision, data_scale, nullable, data_default " +
                "FROM all_tab_columns WHERE owner = ? AND table_name = ? ORDER BY column_id";
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(dstSchema).append(".").append(table).append(" (");
        try (PreparedStatement ps = src.prepareStatement(sql)) {
            ps.setString(1, srcSchema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) sb.append(", ");
                    first = false;
                    String col = rs.getString(1);
                    String dt  = rs.getString(2);
                    int len    = rs.getInt(3);
                    String charUsed = rs.getString(4);
                    int prec   = rs.getInt(5); boolean precNull = rs.wasNull();
                    int scale  = rs.getInt(6); boolean scaleNull = rs.wasNull();
                    String nullable = rs.getString(7);
                    String def = rs.getString(8);

                    sb.append(col).append(" ").append(mapType(dt, len, charUsed, precNull?null:prec, scaleNull?null:scale));
                    if (def != null && !def.isBlank()) sb.append(" DEFAULT ").append(def.trim());
                    if ("N".equalsIgnoreCase(nullable)) sb.append(" NOT NULL");
                }
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private static String mapType(String dt, int len, String charUsed, Integer prec, Integer scale) {
        String t = dt.toUpperCase();
        if (t.equals("NUMBER")) {
            if (prec != null && scale != null) return "NUMBER(" + prec + "," + scale + ")";
            if (prec != null) return "NUMBER(" + prec + ")";
            return "NUMBER";
        }
        if (t.equals("FLOAT")) {
            if (prec != null) return "FLOAT(" + prec + ")";
            return "FLOAT";
        }
        if (t.equals("VARCHAR2") || t.equals("NVARCHAR2") || t.equals("CHAR") || t.equals("NCHAR")) {
            String sem = (charUsed != null && charUsed.equalsIgnoreCase("C")) ? " CHAR" : "";
            return t + "(" + len + sem + ")";
        }
        if (t.equals("RAW")) { return "RAW(" + len + ")"; }
        if (t.startsWith("TIMESTAMP")) { return t; }
        if (t.equals("DATE")) { return "DATE"; }
        return t;
    }

    private static String buildAddPkDDL(Connection src, String srcSchema, String dstSchema, String table) throws SQLException {
        String sql = "SELECT ac.constraint_name FROM all_constraints ac WHERE ac.owner=? AND ac.table_name=? AND ac.constraint_type='P'";
        String pkName = null;
        try (PreparedStatement ps = src.prepareStatement(sql)) {
            ps.setString(1, srcSchema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) pkName = rs.getString(1);
            }
        }
        if (pkName == null) return null;
        String colsSql = "SELECT column_name FROM all_cons_columns WHERE owner=? AND table_name=? AND constraint_name=? ORDER BY position";
        List<String> cols = new ArrayList<>();
        try (PreparedStatement ps = src.prepareStatement(colsSql)) {
            ps.setString(1, srcSchema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            ps.setString(3, pkName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) cols.add(rs.getString(1));
            }
        }
        if (cols.isEmpty()) return null;
        String newPkName = pkName;
        return "ALTER TABLE " + dstSchema + "." + table + " ADD CONSTRAINT " + newPkName +
                " PRIMARY KEY (" + String.join(",", cols) + ")";
    }

    private static List<String> buildCreateIndexesDDL(Connection src, String srcSchema, String dstSchema, String table) throws SQLException {
        String idxSql = "SELECT idx.index_name, idx.uniqueness FROM all_indexes idx WHERE idx.owner=? AND idx.table_name=? AND idx.generated = 'N'";
        List<String> ddls = new ArrayList<>();
        try (PreparedStatement ps = src.prepareStatement(idxSql)) {
            ps.setString(1, srcSchema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String uniq = rs.getString(2);
                    String colSql = "SELECT column_name FROM all_ind_columns WHERE index_owner=? AND index_name=? AND table_name=? ORDER BY column_position";
                    List<String> cols = new ArrayList<>();
                    try (PreparedStatement pcs = src.prepareStatement(colSql)) {
                        pcs.setString(1, srcSchema.toUpperCase());
                        pcs.setString(2, name);
                        pcs.setString(3, table.toUpperCase());
                        try (ResultSet crs = pcs.executeQuery()) {
                            while (crs.next()) cols.add(crs.getString(1));
                        }
                    }
                    if (!cols.isEmpty()) {
                        String uniqStr = "UNIQUE".equalsIgnoreCase(uniq) ? "UNIQUE " : "";
                        String ddl = "CREATE " + uniqStr + "INDEX " + name + " ON " + dstSchema + "." + table +
                                " (" + String.join(",", cols) + ")";
                        ddls.add(ddl);
                    }
                }
            }
        }
        return ddls;
    }

    private static List<String> buildAddFksDDL(Connection src, String srcSchema, String dstSchema, String table) throws SQLException {
        String fkSql = "SELECT ac.constraint_name, acc.position, acc.column_name, r.owner, r.table_name, r.constraint_name " +
                "FROM all_constraints ac " +
                "JOIN all_cons_columns acc ON ac.owner=acc.owner AND ac.constraint_name=acc.constraint_name AND ac.table_name=acc.table_name " +
                "JOIN all_constraints r ON r.owner=ac.r_owner AND r.constraint_name=ac.r_constraint_name " +
                "WHERE ac.owner=? AND ac.table_name=? AND ac.constraint_type='R' " +
                "ORDER BY ac.constraint_name, acc.position";
        Map<String, List<String>> childCols = new LinkedHashMap<>();
        Map<String, String[]> refInfo = new HashMap<>();
        try (PreparedStatement ps = src.prepareStatement(fkSql)) {
            ps.setString(1, srcSchema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String fkName = rs.getString(1);
                    String col = rs.getString(3);
                    String refOwner = rs.getString(4);
                    String refTable = rs.getString(5);
                    String refConstraint = rs.getString(6);
                    childCols.computeIfAbsent(fkName, k -> new ArrayList<>()).add(col);
                    refInfo.put(fkName, new String[]{refOwner, refTable, refConstraint});
                }
            }
        }
        List<String> ddls = new ArrayList<>();
        for (String fk : childCols.keySet()) {
            String[] info = refInfo.get(fk);
            String refOwner = info[0];
            String refTable = info[1];
            String refCons = info[2];
            String pColSql = "SELECT column_name FROM all_cons_columns WHERE owner=? AND table_name=? AND constraint_name=? ORDER BY position";
            List<String> parentCols = new ArrayList<>();
            try (PreparedStatement ps = src.prepareStatement(pColSql)) {
                ps.setString(1, refOwner);
                ps.setString(2, refTable);
                ps.setString(3, refCons);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) parentCols.add(rs.getString(1));
                }
            }
            if (parentCols.isEmpty()) continue;
            String ddl = "ALTER TABLE " + dstSchema + "." + table + " ADD CONSTRAINT " + fk +
                    " FOREIGN KEY (" + String.join(",", childCols.get(fk)) + ") " +
                    "REFERENCES " + dstSchema + "." + refTable + " (" + String.join(",", parentCols) + ")";
            ddls.add(ddl);
        }
        return ddls;
    }

    public static java.util.List<String> getPrimaryKeyColumns(Connection conn, String schema, String table) throws SQLException {
        java.util.List<String> cols = new java.util.ArrayList<>();
        String sql = "SELECT acc.column_name " +
                "FROM all_constraints ac JOIN all_cons_columns acc " +
                "  ON ac.owner=acc.owner AND ac.table_name=acc.table_name AND ac.constraint_name=acc.constraint_name " +
                "WHERE ac.owner=? AND ac.table_name=? AND ac.constraint_type='P' " +
                "ORDER BY acc.position";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) cols.add(rs.getString(1));
            }
        }
        return cols;
    }

    public static class MinMax {
        public final String minLiteral;
        public final String maxLiteral;
        public MinMax(String minLiteral, String maxLiteral) { this.minLiteral = minLiteral; this.maxLiteral = maxLiteral; }
    }

    public static java.util.Map<String, MinMax> getMinMaxForColumns(Connection conn, String schema, String table, java.util.List<String> columns, String whereClause) throws SQLException {
        java.util.Map<String, MinMax> map = new java.util.LinkedHashMap<>();
        if (columns == null || columns.isEmpty()) return map;
        String fq = schema + "." + table;
        for (String col : columns) {
            String sql = "SELECT MIN(" + col + "), MAX(" + col + ") FROM " + fq + (whereClause != null && !whereClause.isBlank() ? " WHERE " + whereClause : "");
            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    Object min = rs.getObject(1);
                    Object max = rs.getObject(2);
                    map.put(col, new MinMax(toSqlLiteral(min), toSqlLiteral(max)));
                }
            }
        }
        return map;
    }

    private static String toSqlLiteral(Object v) {
        if (v == null) return "NULL";
        if (v instanceof java.sql.Timestamp ts) {
            java.time.LocalDateTime ldt = ts.toLocalDateTime();
            String s = ldt.toString(); // ISO-8601
            return "TO_TIMESTAMP('" + s.replace('T',' ') + "','YYYY-MM-DD HH24:MI:SS.FF')";
        }
        if (v instanceof java.sql.Date d) {
            java.time.LocalDate ld = d.toLocalDate();
            String s = ld.toString();
            return "TO_DATE('" + s + "','YYYY-MM-DD')";
        }
        if (v instanceof java.util.Date dt) {
            java.time.Instant inst = dt.toInstant();
            java.time.LocalDateTime ldt = java.time.LocalDateTime.ofInstant(inst, java.time.ZoneOffset.UTC);
            String s = ldt.toString();
            return "TO_TIMESTAMP('" + s.replace('T',' ') + "','YYYY-MM-DD HH24:MI:SS.FF')";
        }
        if (v instanceof Number) {
            return v.toString();
        }
        String s = v.toString().replace("'", "''");
        return "'" + s + "'";
    }

    public static long getTableSizeBytes(Connection conn, String table) throws SQLException {
        final String sql =
                "WITH t AS (SELECT UPPER( '" + table +"' ) AS table_name FROM dual),\n" +
                        "idx AS (SELECT ui.index_name AS segment_name FROM user_indexes ui JOIN t ON ui.table_name = t.table_name),\n" +
                        "lob AS (\n" +
                        "  SELECT ul.segment_name FROM user_lobs ul JOIN t ON ul.table_name = t.table_name\n" +
                        "  UNION ALL\n" +
                        "  SELECT ul.index_name   FROM user_lobs ul JOIN t ON ul.table_name = t.table_name\n" +
                        ")\n" +
                        "SELECT ROUND(SUM(us.bytes), 2) AS total_size_byte\n" +
                        "FROM   user_segments us\n" +
                        "WHERE  (us.segment_type = 'TABLE' AND us.segment_name = (SELECT table_name FROM t))\n" +
                        "   OR  us.segment_name IN (SELECT segment_name FROM idx)\n" +
                        "   OR  us.segment_name IN (SELECT segment_name FROM lob)";

    //    System.out.println("Size SQL: " + sql);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /** Einfache menschenlesbare Formatierung (z. B. "12.3 MB"). */
    public static String humanReadableBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        final String[] units = {"KB","MB","GB","TB","PB","EB"};
        double v = bytes;
        int i = -1;
        while (v >= 1024 && i+1 < units.length) { v /= 1024.0; i++; }
        return String.format(java.util.Locale.ROOT, "%.1f %s", v, units[i]);
    }

}
