package com.example.migrator;

import javafx.fxml.FXML;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

public class ConfigController {
    @FXML private TextField srcUrlField;
    @FXML private TextField srcUserField;
    @FXML private PasswordField srcPassField;
    @FXML private TextField srcSchemaField;

    @FXML private TextField dstUrlField;
    @FXML private TextField dstUserField;
    @FXML private PasswordField dstPassField;
    @FXML private TextField dstSchemaField;

    private DBConfig sourceCfg = new DBConfig();
    private DBConfig targetCfg = new DBConfig();

    public void setSourceCfg(DBConfig cfg) {
        if (cfg != null) this.sourceCfg = cfg;
        srcUrlField.setText(sourceCfg.getUrl());
        srcUserField.setText(sourceCfg.getUser());
        srcPassField.setText(sourceCfg.getPassword());
        srcSchemaField.setText(sourceCfg.getSchema());
    }
    public void setTargetCfg(DBConfig cfg) {
        if (cfg != null) this.targetCfg = cfg;
        dstUrlField.setText(targetCfg.getUrl());
        dstUserField.setText(targetCfg.getUser());
        dstPassField.setText(targetCfg.getPassword());
        dstSchemaField.setText(targetCfg.getSchema());
    }

    public DBConfig getSourceCfg() {
        sourceCfg.setUrl(srcUrlField.getText());
        sourceCfg.setUser(srcUserField.getText());
        sourceCfg.setPassword(srcPassField.getText());
        sourceCfg.setSchema(srcSchemaField.getText());
        try { ConfigStore.save(sourceCfg, targetCfg); } catch (Exception ignored) {}
        return sourceCfg;
    }
    public DBConfig getTargetCfg() {
        targetCfg.setUrl(dstUrlField.getText());
        targetCfg.setUser(dstUserField.getText());
        targetCfg.setPassword(dstPassField.getText());
        targetCfg.setSchema(dstSchemaField.getText());
        try { ConfigStore.save(sourceCfg, targetCfg); } catch (Exception ignored) {}
        return targetCfg;
    }

    @FXML private void onSave() {
        getSourceCfg(); getTargetCfg();
        Stage st = (Stage) srcUrlField.getScene().getWindow();
        st.close();
    }
    @FXML private void onCancel() {
        Stage st = (Stage) srcUrlField.getScene().getWindow();
        st.close();
    }
}
