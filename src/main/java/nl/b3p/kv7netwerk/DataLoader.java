package nl.b3p.kv7netwerk;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
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
    private final Map importRow;
    private String rowLog;
    private String schema;
    private final PrintWriter out;

    public DataLoader(Connection c, int id, PrintWriter out) throws Exception {
        this.c = c;
        this.out = out;

        importRow = new QueryRunner().query(c, "select * from imports where id = ?", new MapHandler(), id);

        init();
    }

    public DataLoader(Connection c, Map importRow, PrintWriter out) throws Exception {
        this.c = c;
        this.out = out;

        this.importRow = importRow;

        init();
    }

    private void init() throws Exception {

        rowLog = (String)importRow.get("log");

        String file = (String)importRow.get("file");

        InputStream is = new FileInputStream(file);
        if(file.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        this.dp = new DataParser(is);
    }

    public void load() throws Exception {

        try {
            if(importRow.get("file_md5") != null) {
                Object duplicate = new QueryRunner().query(c, "select id from imports where id <> ? and file_md5 = ?",
                        new ScalarHandler(),
                        importRow.get("id"),
                        importRow.get("file_md5"));
                if(duplicate != null) {
                    String msg = String.format("Ontvangen bestand #%d, ontvangen op %s heeft zelfde MD5 hash als #%s, negeren",
                            importRow.get("id"),
                            importRow.get("recv_date").toString(),
                            duplicate.toString());
                    log.info(msg);
                    rowLog += "\n" + msg;
                    new QueryRunner().update(c, "update import set state='duplicate', state_date=?, log=? where id=?",
                            new java.sql.Timestamp(System.currentTimeMillis()),
                            rowLog,
                            importRow.get("id"));
                    return;
                }
            }

            String dataDateString = dp.getGroupFields()[7];

            String msg = String.format("Laden #%d ontvangen op %s, datum uit bestand %s...", importRow.get("id"), importRow.get("recv_date").toString(), dataDateString);
            log.info(msg);
            out.println(msg);
            rowLog += "\n" + msg;

            new QueryRunner().update(c, "update import set state='loading', state_date=?, log=? where id=?",
                    new java.sql.Timestamp(System.currentTimeMillis()),
                    rowLog,
                    importRow.get("id"));

            long startTime = System.currentTimeMillis();

            Calendar dataDate = javax.xml.bind.DatatypeConverter.parseDateTime(dataDateString);

            dp.nextTable();
            String[] row = dp.nextRow();
            String dataOwnerCode = row[0];
            String dataOwnerName = row[2];

            schema = dataOwnerCode.toLowerCase() + "_" + importRow.get("id") + "_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(dataDate.getTime());

            new QueryRunner().update(c, "update imports set data_owner = ?, schema = ?, data_date = ? where id = ?", dataOwnerName, schema, new java.sql.Timestamp(dataDate.getTimeInMillis()), importRow.get("id"));

            new QueryRunner().update(c, "drop schema if exists " + schema + " cascade");
            new QueryRunner().update(c, "delete from geometry_columns where f_table_schema = '" + schema + "'");

            new QueryRunner().update(c, "create schema " + schema);
            new QueryRunner().update(c, "set search_path = " + schema + ",data,public");

            insertTables();

            msg = String.format("Geladen in %ss, verwerken geometrie...",
                    (System.currentTimeMillis() - startTime) / 1000.0);
            log.info(msg);
            out.println(msg);
            rowLog += "\n" + msg;

            processGeometry();

            msg = String.format("Klaar in %ss",
                    (System.currentTimeMillis() - startTime) / 1000.0);
            log.info(msg);
            out.println(msg);
            rowLog += "\n" + msg;

            new QueryRunner().update(c, "update imports set state='active', state_date = ?, log=? where id = ?",
                    new java.sql.Timestamp(System.currentTimeMillis()), rowLog, importRow.get("id"));
        } catch(Exception e) {
            log.error("Fout bij verwerken", e);
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            e.printStackTrace(out);
            rowLog += "\n" + "Fout tijdens verwerken: " + sw.toString();
            try {
                new QueryRunner().update(c, "update imports set state='error', state_date = ?, log=? where id = ?",
                        new java.sql.Timestamp(System.currentTimeMillis()), rowLog, importRow.get("id"));
            } catch(Exception e2) {
                log.error("Fout bij updaten error status", e2);
            }
        } finally {
            dp.close();
        }
    }

    private void processGeometry() throws SQLException {
        new QueryRunner().query(c, "select addgeometrycolumn('" + schema + "', 'userstop', 'the_geom', 28992, 'POINT', 2)", new MapHandler());
        new QueryRunner().update(c, "update userstop set the_geom = st_setsrid(st_makepoint(locationx_ew::double precision, locationy_ns::double precision),28992)");
        new QueryRunner().query(c, "select addgeometrycolumn('" + schema + "', 'jopatiminglinkpool', 'the_geom', 28992, 'POINT', 2)", new MapHandler());
        new QueryRunner().update(c, "update jopatiminglinkpool set the_geom = st_setsrid(st_makepoint(locationx_ew::double precision, locationy_ns::double precision),28992)");
        new QueryRunner().update(c, "create table lines as\n" +
            " select lineplanningnumber, journeypatterncode, timinglinkorder::int, st_makeline(the_geom order by distancesincestartoflink::int) as the_geom\n" +
            " from jopatiminglinkpool\n" +
            " group by  lineplanningnumber, journeypatterncode, timinglinkorder::int");
        new QueryRunner().update(c, "alter table lines add column id serial");
        new QueryRunner().update(c, "alter table lines add primary key(id)");

        new QueryRunner().update(c, "insert into geometry_columns(f_table_catalog,f_table_schema,f_table_name,f_geometry_column, coord_dimension,srid,type)\n" +
            "values('','" + schema + "','lines','the_geom',2,28992,'LINESTRING')");

        new QueryRunner().update(c, "create index lines_idx on lines using gist(the_geom)");
        new QueryRunner().update(c, "create index userstop_idx on userstop using gist(the_geom)");

        new QueryRunner().update(c, "update imports set extent = (select st_extent(the_geom) from " + schema + ".lines) where id = ?", importRow.get("id"));
    }

    private void insertTables() throws Exception {
        String[][] table;
        String[] row;
        Set<String> createdTables = new HashSet();

        int totalRows = 0;

        while((table = dp.nextTable()) != null) {
            StringBuilder createTable = new StringBuilder("create table ");
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
            createTable.append(")");
            insert.append(insertValues).append(")");

            if(!createdTables.contains(table[0][0])) {
                new QueryRunner().update(c, createTable.toString());
                createdTables.add(table[0][0]);
            }

            List<Object[]> rows = new ArrayList<Object[]>();

            while((row = dp.nextRow()) != null) {
                rows.add(row);

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
