package com.example.migrator;

import javafx.concurrent.Task;
import java.sql.Connection;

public class MigrationTask extends Task<Void> {
    private final DBConfig srcCfg, dstCfg;
    private final String table;
    private final TableItem item;
    private final int commitBatch;

    public MigrationTask(DBConfig srcCfg, DBConfig dstCfg, String table, TableItem item, int commitBatch) {
        this.srcCfg = srcCfg; this.dstCfg = dstCfg; this.table = table; this.item = item; this.commitBatch = commitBatch;
    }

    @Override
    protected Void call() throws Exception {
        item.setStatus("migrating …");
        try (Connection src = DBManager.open(srcCfg);
             Connection dst = DBManager.open(dstCfg)) {
            String where = WhereStore.loadWhere(srcCfg.getSchema(), table);
            long total = DBManager.countRows(src, srcCfg.getSchema(), table, where);
            DBManager.copyTable(src, srcCfg.getSchema(), dst, dstCfg.getSchema(), table,
                    where, commitBatch,
                    (copied, rate) -> {
                        double frac = total > 0 ? copied / (double) total : 0.0;
                        item.setTransferred((int) copied);
                        item.setProgress(Math.min(1.0, frac));
                        item.setRate(rate);
                        item.setStatus("migrating " + copied + "/" + total);
                    }, item.getStopSignal());
            long newDst = DBManager.countRows(dst, dstCfg.getSchema(), table, where);
            item.setDstCount(Long.toString(newDst));
            item.setStatus("done");
        } catch (Exception ex) {
            item.setStatus("failed: " + ex.getMessage());
            throw ex;
        }
        return null;
    }
}
