package nl.b3p.kv7netwerk;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Matthijs Laan
 */
public class DataProcessorServlet extends HttpServlet {

    private static final Log log = LogFactory.getLog(DataProcessorServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        if(!"127.0.0.1".equals(request.getRemoteAddr())) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
            return;
        }

        long startTime = System.currentTimeMillis();

        StringWriter sw = new StringWriter();
        PrintWriter w = new PrintWriter(sw);
        PrintWriter rw = response.getWriter();
        Object netwerkId = null;
        String msg;
        String status = "error";
        Connection c = null;
        try {
            Context initCtx = new InitialContext();
            DataSource ds = (DataSource)initCtx.lookup("java:comp/env/jdbc/kv7netwerk");
            c = ds.getConnection();

            List<Map<String,Object>> received = new QueryRunner().query(c, "select * from data where state='received'", new MapListHandler());

            if(received.isEmpty()) {
                log.info("Geen nieuwe bestanden ontvangen");
                return;
            }

            msg = "Laden van " + received.size() + " ontvangen bestanden";
            log.info(msg); w.println(msg); rw.println(msg); rw.flush();

            netwerkId = new QueryRunner().query(c, "insert into netwerk(state,processed_date) values (?, ?) returning id",
                    new ScalarHandler(),
                    "processing",
                    new java.sql.Timestamp(System.currentTimeMillis()));

            new QueryRunner().update(c, "update data set state = 'waiting' where state = 'received'");

            String schema = "netwerk_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + netwerkId;

            new QueryRunner().update(c, "drop schema if exists " + schema + " cascade");
            new QueryRunner().update(c, "delete from geometry_columns where f_table_schema = '" + schema + "'");
            new QueryRunner().update(c, "create schema " + schema);
            new QueryRunner().update(c, "set search_path = " + schema + ",data,public");

            new QueryRunner().update(c, "update netwerk set schema = ? where id = ?", schema, netwerkId);
            msg = "Importeren naar schema " + schema;
            log.info(msg); w.println(msg); rw.println(msg); rw.flush();
            for(Map<String,Object> data: received) {
                msg = String.format("Laden bestand %s ontvangen op %s",
                        data.get("filename"),
                        data.get("recv_date").toString());
                log.info(msg); w.println(msg); rw.println(msg); rw.flush();

                new DataLoader(c, data, w).load();
                new QueryRunner().update(c, "update data set netwerk = ? where id = ?", netwerkId, data.get("id"));
            }

            msg = "Alle data geladen in " + DurationFormatUtils.formatPeriod(startTime, System.currentTimeMillis(), "mm:ss") + ", verwerken geometrie...";
            log.info(msg); w.println(msg); rw.println(msg); rw.flush();

            processGeometry(c, schema);

            for(String oldSchema: new QueryRunner().query(c, "select schema from netwerk where state = 'active'", new ColumnListHandler<String>())) {
                msg = "Verwijderen oud schema " + oldSchema;
                log.info(msg); w.println(msg); rw.println(msg); rw.flush();
                new QueryRunner().update(c, "drop schema if exists " + oldSchema + " cascade");
                new QueryRunner().update(c, "update netwerk set state = 'inactive' where id <> ? and state = 'active'", netwerkId);
            }
            status = "active";

            msg = "Verwerking gelukt! Totale tijd " + DurationFormatUtils.formatPeriod(startTime, System.currentTimeMillis(), "mm:ss");
            log.info(msg); w.println(msg); rw.println(msg); rw.flush();
        } catch(Exception e) {
            log.error(e);
            e.printStackTrace(w);
            e.printStackTrace(rw);
        } finally {
            w.flush();
            if(netwerkId != null) {
                try {
                    new QueryRunner().update(c, "update netwerk set state = ?, log = ? where id = ?",
                            status,
                            sw.toString(),
                            netwerkId);
                } catch(Exception e) {
                    log.error("Fout tijdens updaten netwerk record", e);
                }
            }
            if(c != null) {
                DbUtils.closeQuietly(c);
            }
        }
    }

    private void processGeometry(Connection c, String schema) throws SQLException {

        // maak distinct userstop
        new QueryRunner().update(c, "create table map_userstop as\n" +
                "select distinct timingpointname,timingpointtown,locationx_ew,locationy_ns from userstop");
        new QueryRunner().query(c, "select addgeometrycolumn('" + schema + "', 'map_userstop', 'the_geom', 28992, 'POINT', 2)", new MapHandler());
        new QueryRunner().update(c, "update map_userstop set the_geom = st_setsrid(st_makepoint(locationx_ew::double precision, locationy_ns::double precision),28992)");
        new QueryRunner().update(c, "create index map_userstop_idx on map_userstop using gist(the_geom)");

        // voeg punt geometrie toe aan jopatiminglinkpool
        new QueryRunner().query(c, "select addgeometrycolumn('" + schema + "', 'jopatiminglinkpool', 'the_geom', 28992, 'POINT', 2)", new MapHandler());
        new QueryRunner().update(c, "update jopatiminglinkpool set the_geom = st_setsrid(st_makepoint(locationx_ew::double precision, locationy_ns::double precision),28992)");
        new QueryRunner().update(c, "create index jopatiminglinkpool_line on jopatiminglinkpool(dataownercode,lineplanningnumber,journeypatterncode,validfrom)");

        // maak tabel met distinct lijnen joined met info uit line
        new QueryRunner().update(c, "create table map_line as\n" +
            " select distinct j.dataownercode,j.lineplanningnumber,linepublicnumber,linename,transporttype,j.journeypatterncode, validfrom::date\n" +
            " from jopatiminglinkpool j\n" +
            " left join line l on (l.dataownercode=j.dataownercode and l.lineplanningnumber=j.lineplanningnumber)");

        new QueryRunner().update(c, "alter table map_line add column id serial");
        new QueryRunner().update(c, "alter table map_line add primary key(id)");
        new QueryRunner().update(c, "alter table map_line add column validto date");

        // stel validto in op dag voor validfrom van volgend record
        new QueryRunner().update(c, "update map_line ml set validto = \n" +
            "	(select (min(ml2.validfrom) - interval '1 day')::date \n" +
            "	from map_line ml2 \n" +
            "	where ml2.dataownercode=ml.dataownercode \n" +
            "	and ml2.lineplanningnumber=ml.lineplanningnumber \n" +
            "	and ml2.journeypatterncode=ml.journeypatterncode \n" +
            "	and ml2.validfrom > ml.validfrom)");

        new QueryRunner().query(c, "select addgeometrycolumn('" + schema + "', 'map_line', 'the_geom', 28992, 'LINESTRING', 2)", new MapHandler());

        // maak line geometrie op basis van timinglinkorder en distancesincestartoflink
        new QueryRunner().update(c,"update map_line ml\n" +
            "set the_geom = (\n" +
            "	select st_makeline(the_geom order by timinglinkorder::integer, distancesincestartoflink::integer) \n" +
            "	from jopatiminglinkpool j \n" +
            "	where j.dataownercode=ml.dataownercode \n" +
            "	and j.lineplanningnumber=ml.lineplanningnumber \n" +
            "	and j.journeypatterncode=ml.journeypatterncode\n" +
            "	and j.validfrom::date=ml.validfrom\n" +
            "	group by ml.dataownercode,ml.lineplanningnumber,ml.journeypatterncode)\n");

        new QueryRunner().update(c, "create index map_line_idx on map_line using gist(the_geom)");
    }
}
