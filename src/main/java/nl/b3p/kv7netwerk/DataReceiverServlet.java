package nl.b3p.kv7netwerk;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Matthijs Laan
 */
public class DataReceiverServlet extends HttpServlet {

    private static final Log log = LogFactory.getLog(DataReceiverServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, null);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        File dir = new File(getInitParameter("dir"));
        dir.mkdirs();

        File f = File.createTempFile("kv7netwerk_data", ".txt.gz", dir);

        FileOutputStream fos = new FileOutputStream(f);
        GZIPOutputStream gzOut = new GZIPOutputStream(fos);
        try {

            InputStream is = request.getInputStream();
            if("gzip".equals(request.getHeader("Content-Encoding"))) {
                is = new GZIPInputStream(is);
            }
            CountingInputStream cis = new CountingInputStream(is);

            // Lees data en hash vanaf eerste regel die telkens veranderende
            // datum bevat, ook al is de rest van het bestand identiek

            ByteArrayOutputStream firstLine = new ByteArrayOutputStream();
            int c;
            while(true) {
                c = cis.read();
                firstLine.write(c);
                if(c == -1) {
                    log.error("EOF tijdens lezen data");
                    return;
                }
                if(c == (int)'\r') {
                    c = cis.read();
                    firstLine.write(c);
                    if(c != (int)'\n') {
                        log.error("Verwacht LF na CR, ongeldige data");
                        return;
                    }
                    break;
                }
            }

            // Schrijf eerste regel naar bestand
            gzOut.write(firstLine.toByteArray());

            // Maak message digest voor hashen bestand vanaf eerste regel
            MessageDigest md5 = DigestUtils.getMd5Digest();

            // Schrijf de rest van het bestand en update de digest
            byte[] buf = new byte[4096];
            int n = 0;
            while(-1 != (n = cis.read(buf))) {
                gzOut.write(buf, 0, n);
                md5.update(buf, 0, n);
            }

            String hash = new String(Hex.encodeHex(md5.digest()));

            gzOut.flush();

            String logLine = String.format("Ontvangen data van IP %s, %d bytes, hash vanaf tweede regel %s, weggeschreven naar bestand %s",
                    request.getRemoteAddr(),
                    cis.getByteCount(),
                    hash,
                    f.getAbsolutePath());
            Connection conn = null;
            try {
                Context initCtx = new InitialContext();
                DataSource ds = (DataSource)initCtx.lookup("java:comp/env/jdbc/kv7netwerk");
                conn = ds.getConnection();

                java.sql.Timestamp now = new java.sql.Timestamp(new Date().getTime());
                new QueryRunner().update(conn, "insert into data.imports(recv_date, file, file_md5, state, state_date, log) values (?, ?, ?, ?, ?, ?)",
                        now,
                        f.getAbsolutePath(),
                        hash,
                        "received",
                        now,
                        logLine + "\n"
                );
            } catch(Exception e) {
                log.error("Fout bij updaten imports tabel in database", e);
            } finally {
                if(conn != null) {
                    DbUtils.closeQuietly(conn);
                }
            }

            log.info(logLine);
        } catch(IOException e) {
            log.error("Fout bij schrijven", e);
        } finally {
            gzOut.close();
        }

        response.sendError(HttpServletResponse.SC_NO_CONTENT, "No content");
    }

    @Override
    public String getServletInfo() {
        return "Servlet voor ontvangen van KV7netwerk data";
    }
}
