package com.example.migrator;

import javafx.beans.property.*;

public class TableItem {

    private final StringProperty tableName = new SimpleStringProperty();
    private final StringProperty srcCount = new SimpleStringProperty("");
    private final StringProperty dstCount = new SimpleStringProperty("");
    private final StringProperty status   = new SimpleStringProperty("");

    private final IntegerProperty transferred = new SimpleIntegerProperty(0);
    private final DoubleProperty  rate        = new SimpleDoubleProperty(0.0);
    private final DoubleProperty  progress    = new SimpleDoubleProperty(0.0);

    private DBManager.StopSignal stopSignal = new DBManager.StopSignal();
    private final BooleanProperty deleting = new SimpleBooleanProperty(false);

    private final StringProperty  size        = new SimpleStringProperty("n/a");
    private final DoubleProperty sizeMB = new SimpleDoubleProperty();

    public TableItem(String table) { this.tableName.set(table); }
    public TableItem(String table, String srcCount, String dstCount) {
        this.tableName.set(table);
        this.srcCount.set(srcCount == null ? "" : srcCount);
        this.dstCount.set(dstCount == null ? "" : dstCount);
    }

    public String getTableName() { return tableName.get(); }
    public void setTableName(String v) { tableName.set(v); }
    public StringProperty tableNameProperty() { return tableName; }

    public String getSrcCount() { return srcCount.get(); }
    public void setSrcCount(String v) { srcCount.set(v); }
    public StringProperty srcCountProperty() { return srcCount; }

    public String getDstCount() { return dstCount.get(); }
    public void setDstCount(String v) { dstCount.set(v); }
    public StringProperty dstCountProperty() { return dstCount; }

    public String getStatus() { return status.get(); }
    public void setStatus(String v) { status.set(v); }
    public StringProperty statusProperty() { return status; }

    public int getTransferred() { return transferred.get(); }
    public void setTransferred(int v) { transferred.set(v); }
    public IntegerProperty transferredProperty() { return transferred; }

    public double getRate() { return rate.get(); }
    public void setRate(double v) { rate.set(v); }
    public DoubleProperty rateProperty() { return rate; }

    public double getProgress() { return progress.get(); }
    public void setProgress(double v) { progress.set(v); }
    public DoubleProperty progressProperty() { return progress; }

    public DBManager.StopSignal getStopSignal() { return stopSignal; }
    public void setStopSignal(DBManager.StopSignal s) { this.stopSignal = s; }

    public boolean isDeleting() { return deleting.get(); }
    public void setDeleting(boolean v) { deleting.set(v); }
    public BooleanProperty deletingProperty() { return deleting; }

    public String getSize() { return size.get(); }
    public void setSize(String v) { size.set(v); }
    public StringProperty sizeProperty() { return size; }

    public DoubleProperty sizeMBProperty() { return sizeMB; }
    public double getSizeMB() { return sizeMB.get(); }
    public void setSizeMB(double v) { sizeMB.set(v); }

}
