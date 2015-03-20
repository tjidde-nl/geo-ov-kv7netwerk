package nl.b3p.kv7netwerk;

import java.io.IOException;
import java.sql.Connection;
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
import org.apache.commons.dbutils.handlers.MapListHandler;

/**
 *
 * @author Matthijs Laan
 */
public class DataProcessorServlet extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        if(!"127.0.0.1".equals(request.getRemoteAddr())) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
            return;
        }

        Connection c = null;
        try {
            Context initCtx = new InitialContext();
            DataSource ds = (DataSource)initCtx.lookup("java:comp/env/jdbc/kv7netwerk");
            c = ds.getConnection();

            List<Map<String,Object>> received = new QueryRunner().query(c, "select * from imports where state='received'", new MapListHandler());

            if(!received.isEmpty()) {
                for(Map<String,Object> i: received) {
                    response.getWriter().printf("Verwerken #%d, bestand %s ontvangen op %s\n",
                            i.get("id"),
                            i.get("file"),
                            i.get("recv_date").toString());

                    response.getWriter().flush();
                    new DataLoader(c, i, response.getWriter()).load();
                    response.getWriter().flush();
                }
            }
        } catch(Exception e) {
            e.printStackTrace(response.getWriter());
        } finally {
            if(c != null) {
                DbUtils.closeQuietly(c);
            }
        }
    }


}
