package com.example.migrator;

import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.ContextMenuEvent;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainController {
    // Linke Seite
    @FXML private ListView<String> availableTablesList;
    @FXML private Button addButton, removeButton, addAllButton, removeAllButton;
    @FXML private Button configButton, loadTablesButton, startButton;
    @FXML private TextField batchSizeField;

    // Tabelle
    @FXML private TableView<TableItem> overviewTable;
    @FXML private TableColumn<TableItem, String> colTable, colSrcCount, colDstCount, colStatus;
    @FXML private TableColumn<TableItem, Number> colTransferred, colRate;
    @FXML private TableColumn<TableItem, Double> colProgress;
    @FXML private TableColumn<TableItem, String> colSize;        // NEU: Größe
    @FXML private TableColumn<TableItem, Void> colAction;        // NEU: eine Spalte für alle Aktionen

    // Datenmodelle
    private final ObservableList<String> availableTables = FXCollections.observableArrayList();
    private final ObservableList<TableItem> tableModels = FXCollections.observableArrayList();

    // Threadpool: max. 5 parallel
    private final ExecutorService executor = Executors.newFixedThreadPool(5, r -> {
        Thread t = new Thread(r, "count-pool");
        t.setDaemon(true);
        return t;
    });

    private final DBConfig sourceCfg = new DBConfig();
    private final DBConfig targetCfg = new DBConfig();

    @FXML
    public void initialize() {
        try { ConfigStore.loadInto(sourceCfg, targetCfg); } catch (IOException e) { e.printStackTrace(); }

        // Listen/Selektion
        availableTablesList.setItems(availableTables);
        availableTablesList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        // Buttons aktivieren/deaktivieren
        addButton.disableProperty().bind(Bindings.isEmpty(availableTablesList.getSelectionModel().getSelectedItems()));
        removeButton.disableProperty().bind(Bindings.isEmpty(overviewTable.getSelectionModel().getSelectedItems()));
        addAllButton.disableProperty().bind(Bindings.isEmpty(availableTables)); // Add All nur, wenn etwas geladen ist

        // Spalten-Bindings
        colTable.setCellValueFactory(c -> c.getValue().tableNameProperty());
        colSrcCount.setCellValueFactory(c -> c.getValue().srcCountProperty());
        colDstCount.setCellValueFactory(c -> c.getValue().dstCountProperty());
        colTransferred.setCellValueFactory(c -> c.getValue().transferredProperty());
        colRate.setCellValueFactory(c -> c.getValue().rateProperty());
        colProgress.setCellValueFactory(c -> c.getValue().progressProperty().asObject());
        colStatus.setCellValueFactory(c -> c.getValue().statusProperty());
        colSize.setCellValueFactory(c -> c.getValue().sizeProperty()); // NEU

        // Progress als ProgressBar rendern
        colProgress.setCellFactory(tc -> new TableCell<>() {
            private final ProgressBar bar = new ProgressBar(0);
            { setContentDisplay(ContentDisplay.GRAPHIC_ONLY); bar.setMaxWidth(Double.MAX_VALUE); }
            @Override protected void updateItem(Double v, boolean empty) {
                super.updateItem(v, empty);
                if (empty || getIndex()<0 || getIndex()>=getTableView().getItems().size()) { setGraphic(null); return; }
                TableItem item = getTableView().getItems().get(getIndex());
                bar.setProgress(v==null?0:v);
                bar.setStyle(item!=null && item.isDeleting()? "-fx-accent: #e53935;" : "");
                setGraphic(bar);
            }
        });

        overviewTable.setItems(tableModels);

        // Button-Handler
        configButton.setOnAction(this::openConfig);
        loadTablesButton.setOnAction(this::loadTables);
        startButton.setOnAction(this::startOverview);
        addButton.setOnAction(e -> addSelectedToOverview());
        addAllButton.setOnAction(e -> addAllToOverview());
        removeButton.setOnAction(e -> removeSelectedFromOverview());
        removeAllButton.setOnAction(e -> clearOverview());

        // Spalte mit den Aktionen
        setupActionColumn();

        // Kontextmenü für WHERE
        addWhereContextMenuOnOverview();
    }

    /** Eine Aktionsspalte mit mehreren Buttons (Copy/Stop/Show/Delete/Create). */
    private void setupActionColumn() {
        colAction.setSortable(false);
        colAction.setReorderable(false);
        colAction.setCellFactory(col -> new TableCell<>() {
            private final Button btnMigrate   = new Button("Copy");
            private final Button btnStop      = new Button("Stop");
            private final Button btnShowWhere = new Button("Show Condition");
            private final Button btnDeleteDst = new Button("Delete");
            private final Button btnCreateDst = new Button("Create Table");

            private final HBox box = new HBox(6, btnMigrate, btnStop, btnShowWhere, btnDeleteDst, btnCreateDst);

            {
                btnMigrate.setOnAction(e -> { TableItem item = getSafeRowItem(); if (item != null) startMigration(item); });
                btnStop.setOnAction(e -> {
                    TableItem item = getSafeRowItem();
                    if (item != null) {
                        item.setStatus("stopping");
                        if (item.getStopSignal()!=null) item.getStopSignal().stop();
                    }
                });
                btnShowWhere.setOnAction(e -> { TableItem item = getSafeRowItem(); if (item != null) onShowWhere(item); });
                btnDeleteDst.setOnAction(e -> { TableItem item = getSafeRowItem(); if (item != null) onDeleteTarget(item); });
                btnCreateDst.setOnAction(e -> { TableItem item = getSafeRowItem(); if (item != null) onCreateTarget(item); });
            }

            private TableItem getSafeRowItem() {
                int idx = getIndex();
                if (idx < 0 || idx >= getTableView().getItems().size()) return null;
                return getTableView().getItems().get(idx);
            }

            @Override
            protected void updateItem(Void v, boolean empty) {
                super.updateItem(v, empty);
                if (empty) { setGraphic(null); return; }
                TableItem item = getSafeRowItem();
                if (item == null) { setGraphic(null); return; }
                boolean whereExists = WhereStore.hasWhere(sourceCfg.getSchema(), item.getTableName());
                btnShowWhere.setDisable(!whereExists);
                setGraphic(box);
            }
        });
    }

    /** WHERE-Anzeige (Informationsdialog). */
    private void onShowWhere(TableItem item) {
        String table = item.getTableName();
        String current = WhereStore.loadWhere(sourceCfg.getSchema(), table);
        Alert a = new Alert(Alert.AlertType.INFORMATION);
        a.setTitle("WHERE");
        a.setHeaderText("Tabelle: " + table + " (Quelle: " + sourceCfg.getSchema() + ")");
        a.setContentText(current==null || current.isBlank()? "(keine WHERE-Bedingung hinterlegt)": current);
        a.getDialogPane().setMinHeight(Region.USE_PREF_SIZE);
        a.showAndWait();
    }

    /** Kontextmenü pro Zeile: WHERE bearbeiten + Vorschlag. */
    private void addWhereContextMenuOnOverview() {
        overviewTable.setRowFactory(tv -> {
            TableRow<TableItem> row = new TableRow<>();
            ContextMenu cm = new ContextMenu();
            MenuItem edit = new MenuItem("WHERE bearbeiten…");
            edit.setOnAction(ev -> {
                TableItem ti = row.getItem(); if (ti==null) return;
                final String table = ti.getTableName();
                String current = WhereStore.loadWhere(sourceCfg.getSchema(), table);

                TextArea ta = new TextArea(current == null ? "" : current);
                ta.setPrefRowCount(8);
                ta.setPromptText("Bedingung ohne WHERE (z. B. ID BETWEEN 100 AND 200)");

                Button btnSuggest = new Button("Vorschlag einfügen");
                Label info = new Label(); info.setStyle("-fx-text-fill: -fx-accent;");
                btnSuggest.setOnAction(evt -> {
                    btnSuggest.setDisable(true);
                    String oldText = btnSuggest.getText();
                    btnSuggest.setText("ermittle …");
                    Task<String> suggestTask = new Task<>() {
                        @Override protected String call() throws Exception {
                            try (Connection src = DBManager.open(sourceCfg)) {
                                List<String> pkCols = DBManager.getPrimaryKeyColumns(src, sourceCfg.getSchema(), table);
                                if (pkCols.isEmpty()) {
                                    try (var st = src.createStatement();
                                         var rs = st.executeQuery("SELECT * FROM " + sourceCfg.getSchema() + "." + table + " WHERE 1=0")) {
                                        var md = rs.getMetaData();
                                        if (md.getColumnCount() > 0) {
                                            pkCols = new java.util.ArrayList<>();
                                            pkCols.add(md.getColumnName(1));
                                        }
                                    }
                                }
                                String baseWhere = ta.getText(); baseWhere = (baseWhere == null ? "" : baseWhere.trim());
                                var mm = DBManager.getMinMaxForColumns(src, sourceCfg.getSchema(), table, pkCols, baseWhere);
                                StringBuilder sb = new StringBuilder();
                                boolean first = true;
                                for (String col : pkCols) {
                                    var m = mm.get(col);
                                    if (m == null) continue;
                                    if (!first) sb.append(" AND ");
                                    first = false;
                                    sb.append(col).append(" BETWEEN ").append(m.minLiteral).append(" AND ").append(m.maxLiteral);
                                }
                                return sb.toString();
                            }
                        }
                    };
                    suggestTask.setOnSucceeded(ok -> {
                        String s = suggestTask.getValue();
                        if (s != null && !s.isBlank()) {
                            ta.setText(s);
                            info.setText("Vorschlag eingefügt.");
                        } else {
                            info.setText("Kein Vorschlag verfügbar.");
                        }
                        btnSuggest.setDisable(false);
                        btnSuggest.setText(oldText);
                    });
                    suggestTask.setOnFailed(fail -> {
                        info.setText("Fehler: " + suggestTask.getException().getMessage());
                        btnSuggest.setDisable(false);
                        btnSuggest.setText(oldText);
                    });
                    executor.submit(suggestTask);
                });

                VBox content = new VBox(8, ta, new HBox(8, btnSuggest, info));
                content.setMinWidth(560);

                Dialog<String> dlg = new Dialog<>();
                dlg.setTitle("WHERE-Bedingung");
                dlg.setHeaderText("Tabelle: " + table + " (Quelle: " + sourceCfg.getSchema() + ")");
                dlg.getDialogPane().setContent(content);
                dlg.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
                dlg.setResultConverter(bt -> bt == ButtonType.OK ? ta.getText().trim() : null);

                String res = dlg.showAndWait().orElse(null);
                if (res != null) {
                    try { WhereStore.saveWhere(sourceCfg.getSchema(), table, res); } catch (Exception ex) { ex.printStackTrace(); }
                    overviewTable.refresh();
                    runCountsForItems(List.of(ti));
                }
            });
            cm.getItems().add(edit);
            row.setOnContextMenuRequested((ContextMenuEvent e) -> { if (!row.isEmpty()) cm.show(row, e.getScreenX(), e.getScreenY()); });
            row.setOnMousePressed(e -> cm.hide());
            return row;
        });
    }

    // --- Buttons links -------------------------------------------------------

    private void openConfig(ActionEvent e) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("config.fxml"));
            VBox pane = loader.load();
            ConfigController ctrl = loader.getController();
            ctrl.setSourceCfg(sourceCfg);
            ctrl.setTargetCfg(targetCfg);

            Stage dialog = new Stage();
            dialog.initModality(Modality.APPLICATION_MODAL);
            dialog.setTitle("Verbindungsdaten");
            dialog.setScene(new Scene(pane));
            dialog.showAndWait();
        } catch (Exception ex) {
            showError("Konfiguration konnte nicht geöffnet werden", ex);
        }
    }

    private void loadTables(ActionEvent e) {
        Task<List<String>> task = new Task<>() {
            @Override protected List<String> call() throws Exception {
                try (Connection src = DBManager.open(sourceCfg)) {
                    return DBManager.listTables(src, sourceCfg.getSchema());
                }
            }
        };
        task.setOnSucceeded(ev -> availableTables.setAll(task.getValue()));
        task.setOnFailed(ev -> showError("Tabellen laden fehlgeschlagen", task.getException()));
        executor.submit(task);
    }

    private void startOverview(ActionEvent e) {
        runCountsForItems(tableModels);
    }

    // --- Aktionen in der Tabelle --------------------------------------------

    private void startMigration(TableItem item) {
        item.getStopSignal().reset();
        int batch = 1000;
        try {
            if (batchSizeField.getText()!=null && !batchSizeField.getText().isBlank())
                batch = Integer.parseInt(batchSizeField.getText().trim());
        } catch (NumberFormatException ex) { batch = 1000; }

        MigrationTask task = new MigrationTask(sourceCfg, targetCfg, item.getTableName(), item, batch);
        task.setOnFailed(ev -> {
            Throwable ex = task.getException();
            item.setStatus("error: " + (ex!=null?ex.getMessage():"unknown"));
        });
        executor.submit(task);
    }

    private void onCreateTarget(TableItem item) {
        String table = item.getTableName();
        Task<Void> t = new Task<>() {
            @Override protected Void call() {
                try (Connection src = DBManager.open(sourceCfg); Connection dst = DBManager.open(targetCfg)) {
                    boolean exists = DBManager.tableExists(dst, targetCfg.getSchema(), table);
                    if (!exists) {
                        DBManager.createTableLikeSource(src, sourceCfg.getSchema(), dst, targetCfg.getSchema(), table);
                        Platform.runLater(() -> item.setStatus("target created"));
                    } else Platform.runLater(() -> item.setStatus("target exists"));
                    Platform.runLater(() -> runCountsForItems(List.of(item)));
                } catch (Exception ex) {
                    Platform.runLater(() -> item.setStatus("create failed: " + ex.getMessage()));
                }
                return null;
            }
        };
        t.setOnFailed(ev -> item.setStatus("create failed: " + t.getException().getMessage()));
        executor.submit(t);
    }

    private void onDeleteTarget(TableItem item) {
        final String table = item.getTableName();
        final String where = WhereStore.loadWhere(sourceCfg.getSchema(), table);

        item.getStopSignal().reset();

        final long toDelete;
        try (Connection dst = DBManager.open(targetCfg)) {
            toDelete = DBManager.countRows(dst, targetCfg.getSchema(), table, where);
        } catch (Exception ex) { showAlert(Alert.AlertType.ERROR, "Fehler beim Zählen im Ziel", ex.getMessage()); return; }

        if (toDelete == 0) { showAlert(Alert.AlertType.INFORMATION, "Keine Zeilen zu löschen", "0 Zeilen (WHERE)"); return; }

        int batchSize = 1000;
        try {
            if (batchSizeField.getText()!=null && !batchSizeField.getText().isBlank())
                batchSize = Integer.parseInt(batchSizeField.getText().trim());
        } catch (NumberFormatException ignore) {}

        item.setDeleting(true); item.setStatus("deleting …"); item.setTransferred(0); item.setProgress(0);

        final TableItem itemRef = item;
        final int commitBatchRef = batchSize;
        final long startedRef = System.nanoTime();
        final long toDeleteRef = toDelete;
        final String whereRef = where;

        Task<Void> delTask = new Task<>() {
            @Override protected Void call() throws Exception {
                try (Connection dst = DBManager.open(targetCfg)) {
                    DBManager.DeleteListener listener = deletedSoFar -> {
                        final double frac = toDeleteRef > 0 ? (double)deletedSoFar/(double)toDeleteRef : 0.0;
                        final double sec = (System.nanoTime()-startedRef)/1_000_000_000.0;
                        final double rate = Math.round(sec>0 ? deletedSoFar/sec : 0);
                        Platform.runLater(() -> {
                            itemRef.setTransferred((int)deletedSoFar);
                            itemRef.setProgress(Math.min(1.0, frac));
                            itemRef.setRate(rate);
                            itemRef.setStatus("deleting " + deletedSoFar + "/" + toDeleteRef + " (batch " + commitBatchRef + ")");
                        });
                    };
                    final int affected = DBManager.deleteRowsInBatches(dst, targetCfg.getSchema(), table, whereRef, commitBatchRef, listener, item.getStopSignal());
                    long remaining; try { remaining = DBManager.countRows(dst, targetCfg.getSchema(), table, whereRef); } catch (Exception ex) { remaining = -1; }
                    final long remainingRef = remaining;
                    Platform.runLater(() -> {
                        itemRef.setDeleting(false);
                        itemRef.setStatus("deleted " + affected + " (target)");
                        if (remainingRef >= 0) itemRef.setDstCount(Long.toString(remainingRef));
                        itemRef.setProgress(0);
                    });
                } catch (Exception ex) {
                    Platform.runLater(() -> { itemRef.setDeleting(false); itemRef.setStatus("delete failed: " + ex.getMessage()); });
                }
                return null;
            }
        };
        delTask.setOnFailed(ev -> { itemRef.setDeleting(false); itemRef.setStatus("delete failed: " + delTask.getException().getMessage()); });
        executor.submit(delTask);
    }

    // --- Add/Remove der Übersicht -------------------------------------------

    private void addSelectedToOverview() {
        var sel = new ArrayList<>(availableTablesList.getSelectionModel().getSelectedItems());
        if (sel.isEmpty()) {
            showAlert(Alert.AlertType.INFORMATION, "Keine Auswahl", "Bitte zuerst eine oder mehrere Tabellen links auswählen.");
            return;
        }
        int before = tableModels.size();
        List<TableItem> newly = new ArrayList<>();
        for (String t : sel) {
            if (tableModels.stream().noneMatch(m -> m.getTableName().equalsIgnoreCase(t))) {
                TableItem itm = new TableItem(t);
                tableModels.add(itm);
                newly.add(itm);
            }
        }
        overviewTable.refresh();
        int added = tableModels.size() - before;
        if (added == 0) {
            showAlert(Alert.AlertType.INFORMATION, "Schon vorhanden", "Alle ausgewählten Tabellen sind bereits in der Übersicht.");
            return;
        }
        overviewTable.scrollTo(tableModels.size() - 1);
        if (!newly.isEmpty()) runCountsForItems(newly);
    }

    private void addAllToOverview() {
        if (availableTables.isEmpty()) {
            showAlert(Alert.AlertType.INFORMATION, "Keine Tabellen geladen", "Bitte zuerst auf \"Tabellen laden\" klicken.");
            return;
        }
        int before = tableModels.size();
        List<TableItem> newly = new ArrayList<>();
        for (String t : availableTables) {
            if (tableModels.stream().noneMatch(m -> m.getTableName().equalsIgnoreCase(t))) {
                TableItem itm = new TableItem(t);
                tableModels.add(itm);
                newly.add(itm);
            }
        }
        overviewTable.refresh();
        int added = tableModels.size() - before;
        if (added == 0) {
            showAlert(Alert.AlertType.INFORMATION, "Schon vollständig", "Alle geladenen Tabellen sind bereits in der Übersicht.");
            return;
        }
        overviewTable.scrollTo(tableModels.size() - 1);
        if (!newly.isEmpty()) runCountsForItems(newly);
    }

    private void removeSelectedFromOverview() {
        var selItems = new ArrayList<>(overviewTable.getSelectionModel().getSelectedItems());
        tableModels.removeAll(selItems);
        overviewTable.refresh();
    }

    private void clearOverview() {
        tableModels.clear();
        overviewTable.refresh();
    }

    // --- Zähl-/Größen-Tasks --------------------------------------------------

    private void runCountsForItems(Collection<TableItem> items) {
        for (TableItem it : items) {
            if (it == null || it.getTableName() == null) continue;
            final TableItem item = it; final String tbl = it.getTableName();
            Task<Void> t = new Task<>() {
                @Override protected Void call() {
                    // Quelle zählen + Größe ermitteln
                    try (Connection src = DBManager.open(sourceCfg)) {
                        final String where = WhereStore.loadWhere(sourceCfg.getSchema(), tbl);
                        final long c = DBManager.countRows(src, sourceCfg.getSchema(), tbl, where);
                        Platform.runLater(() -> item.setSrcCount(Long.toString(c)));

                        try {
                            long bytes = DBManager.getTableSizeBytes(src, tbl);
                            String nice = DBManager.humanReadableBytes(bytes);
                            Platform.runLater(() -> item.setSize(nice));
                        } catch (Exception ex) {
                            Platform.runLater(() -> item.setSize("n/a")); // statt "error"
                            System.out.println("Fehler: " + ex.getMessage());
                        }

                    } catch (Exception ex) {
                        Platform.runLater(() -> item.setSrcCount("error"));
                    }
                    // Ziel zählen
                    try (Connection dst = DBManager.open(targetCfg)) {
                        final String where2 = WhereStore.loadWhere(sourceCfg.getSchema(), tbl);
                        final boolean exists = DBManager.tableExists(dst, targetCfg.getSchema(), tbl);
                        if (!exists) {
                            Platform.runLater(() -> {
                                item.setDstCount("missing");

                            });
                        } else {
                            final long c2 = DBManager.countRows(dst, targetCfg.getSchema(), tbl, where2);
                            Platform.runLater(() -> item.setDstCount(Long.toString(c2)));

                        }
                    } catch (Exception ex) {
                        Platform.runLater(() -> item.setDstCount("error"));
                        System.out.println("Fehler: " + ex.getMessage());
                    }
                    return null;
                }
            };
            executor.submit(t);
        }
    }

    // --- Dialog-Helfer -------------------------------------------------------

    private void showError(String header, Throwable ex) {
        ex.printStackTrace();
        showAlert(Alert.AlertType.ERROR, header, ex.getMessage());
    }
    private void showAlert(Alert.AlertType type, String header, String content) {
        Alert a = new Alert(type);
        a.setHeaderText(header);
        a.setContentText(content);
        a.showAndWait();
    }
}
