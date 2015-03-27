package nl.b3p.kv7netwerk;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Matthijs Laan
 */
public class DataLoader {
    private static final Log log = LogFactory.getLog(DataLoader.class);

    private final Connection c;
    private DataParser dp;
    private final Map dataRow;
    private String rowLog;
    private final PrintWriter out;

    public DataLoader(Connection c, int id, PrintWriter out) throws Exception {
        this.c = c;
        this.out = out;

        dataRow = new QueryRunner().query(c, "select * from data where id = ?", new MapHandler(), id);

        init();
    }

    public DataLoader(Connection c, Map dataRow, PrintWriter out) throws Exception {
        this.c = c;
        this.out = out;

        this.dataRow = dataRow;

        init();
    }

    private void init() throws Exception {

        rowLog = (String)dataRow.get("log");

        String filename = (String)dataRow.get("filename");

        InputStream is = new FileInputStream(filename);
        if(filename.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        this.dp = new DataParser(is);
    }

    public void load() throws Exception {

        try {
            String dataDateString = dp.getGroupFields()[7];

            String msg = String.format("Laden bestand %s ontvangen op %s, datum uit bestand %s...", dataRow.get("filename"), dataRow.get("recv_date").toString(), dataDateString);
            log.info(msg);
            out.println(msg);
            rowLog += "\n" + msg;

            new QueryRunner().update(c, "update data set state='loading', state_date=?, log=? where id=?",
                    new java.sql.Timestamp(System.currentTimeMillis()),
                    rowLog,
                    dataRow.get("id"));

            long startTime = System.currentTimeMillis();

            Calendar dataDate = javax.xml.bind.DatatypeConverter.parseDateTime(dataDateString);

            dp.nextTable();
            String[] row = dp.nextRow();
            String dataOwnerName = row[2];

            new QueryRunner().update(c, "update data set data_owner = ?, data_date = ? where id = ?", dataOwnerName, new java.sql.Timestamp(dataDate.getTimeInMillis()), dataRow.get("id"));

            insertTables();

            msg = "Geladen in " + DurationFormatUtils.formatPeriod(startTime, System.currentTimeMillis(), "mm:ss");
            log.info(msg);
            out.println(msg);
            rowLog += "\n" + msg;

            new QueryRunner().update(c, "update data set state='loaded', state_date = ?, log=? where id = ?",
                    new java.sql.Timestamp(System.currentTimeMillis()), rowLog, dataRow.get("id"));
        } catch(Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            rowLog += "\n" + "Fout tijdens verwerken: " + sw.toString();
            try {
                new QueryRunner().update(c, "update data set state='error', state_date = ?, log=? where id = ?",
                        new java.sql.Timestamp(System.currentTimeMillis()), rowLog, dataRow.get("id"));
            } catch(Exception e2) {
                log.error("Fout bij updaten error status", e2);
            }
            throw e;
        } finally {
            dp.close();
        }
    }

    private void insertTables() throws Exception {
        String[][] table;
        String[] row;
        Set<String> createdTables = new HashSet();

        int totalRows = 0;

        while((table = dp.nextTable()) != null) {
            StringBuilder createTable = new StringBuilder("create table if not exists ");
            StringBuilder insert = new StringBuilder("insert into ").append(table[0][0].toLowerCase()).append("(");
            StringBuilder insertValues = new StringBuilder(") values(");
            createTable.append(table[0][0].toLowerCase())
                    .append(" (");
            for(int i = 0; i < table[1].length; i++) {
                if(i > 0) {
                    createTable.append(", ");
                    insert.append(", ");
                    insertValues.append(", ");
                }
                createTable.append(table[1][i]).append(" varchar");
                insert.append(table[1][i]);
                insertValues.append("?");
            }
            createTable.append(", data_id integer, row_num integer)");
            insert.append(", data_id, row_num");
            insertValues.append(", ?, ?");
            insert.append(insertValues).append(")");

            if(!createdTables.contains(table[0][0])) {
                new QueryRunner().update(c, createTable.toString());
                createdTables.add(table[0][0]);
            }

            List<Object[]> rows = new ArrayList<Object[]>();

            int rowNum = 0;
            while((row = dp.nextRow()) != null) {
                Object[] rowDataId = new Object[row.length+2];
                System.arraycopy(row, 0, rowDataId, 0, row.length);
                rowDataId[rowDataId.length-2] = dataRow.get("id");
                rowDataId[rowDataId.length-1] = rowNum++;
                rows.add(rowDataId);

                if(rows.size() == 100) {
                    new QueryRunner().batch(c, insert.toString(), rows.toArray(new Object[][] {}));
                    totalRows += rows.size();
                    rows = new ArrayList<Object[]>();
                }
            }
            if(!rows.isEmpty()) {
                new QueryRunner().batch(c, insert.toString(), rows.toArray(new Object[][] {}));
                totalRows += rows.size();
            }
        }
        String msg = String.format("%d rijen geinsert", totalRows);
        log.debug(msg);
        out.println(msg);
        rowLog += "\n" + msg;
    }
}
