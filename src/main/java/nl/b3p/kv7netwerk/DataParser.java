package nl.b3p.kv7netwerk;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 *
 * @author Matthijs Laan
 */
public class DataParser {

    private final BufferedReader reader;

    private String pushedBackLine;

    private String[] groupFields, tableInfo, tableFields, row;

    public DataParser(InputStream is) throws Exception {
        reader = new BufferedReader(new InputStreamReader(is));

        readGroup();
    }

    private String nextLine() throws IOException {
        if(pushedBackLine != null) {
            String l = pushedBackLine;
            pushedBackLine = null;
            return l;
        }
        String l = reader.readLine();
        while(l != null && l.trim().length() == 0) {
            l = reader.readLine();
        }
        return l == null ? null : l.trim();
    }

    public void close() throws IOException {
        reader.close();
    }

    private static String[] parseFields(String l) {
        String[] split = l.split("\\|");

        for(int i = 0; i < split.length; i++) {
            String s = split[i];
            if("\\0".equals(s)) {
                split[i] = null;
                continue;
            }
            split[i] = s.replace("\\r", "\r").replace("\\n", "\n").replace("\\i", "\\").replace("\\p", "|");
        }
        return split;
    }

    private void readGroup() throws Exception {
        String l = nextLine();

        if(l == null || !l.startsWith("\\G")) {
            throw new Exception("Ongeldig data formaat, verwacht regel met \\G, gelezen " + l);
        }

        groupFields = parseFields(l.substring(2));
    }

    public String[][] nextTable() throws Exception {
        String l = nextLine();

        while(l != null && !l.startsWith("\\T")) {
            l = nextLine();
        }
        if(l == null) {
            return null;
        }
        tableInfo = parseFields(l.substring(2));
        l = nextLine();
        if(l == null || !l.startsWith("\\L")) {
            throw new Exception("Ongeldig data formaat, verwacht regel met \\L, gelezen " + l);
        }
        tableFields = parseFields(l.substring(2));

        return new String[][] { tableInfo, tableFields };
    }

    public String[] nextRow() throws Exception {
        String l = nextLine();
        if(l == null) {
            return null;
        }
        if(l.startsWith("\\T")) {
            pushedBackLine = l;
            return null;
        }
        row = parseFields(l);
        return row;
    }


    public String[] getGroupFields() {
        return groupFields;
    }

    public String[] getTableInfo() {
        return tableInfo;
    }

    public String[] getTableFields() {
        return tableFields;
    }

    public String[] getRow() {
        return row;
    }

    public static void main(String[] args) throws Exception {
        DataParser dp = new DataParser(new FileInputStream("/home/matthijsln/1157-2015-03-02_22_51_25.txt"));

        System.out.println("Group: " + Arrays.toString(dp.getGroupFields()));

        String[][] table;
        while((table = dp.nextTable()) != null) {
            System.out.println("Table: " + Arrays.toString(table[0]) + ", fields: " + Arrays.toString(table[1]));

            String[] row;
            while((row = dp.nextRow()) != null) {
                System.out.println("  " + Arrays.toString(row));
            }
        }
    }

}
