package com.karatebancho.h2tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.h2.tools.SimpleResultSet;

public class LTSV {
    public static ResultSet read(Connection conn, String fileName) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
        String record;
        SimpleResultSet rs = new SimpleResultSet();
        try {
            boolean isFirstRecord = true;
            while ((record = reader.readLine()) != null) {
                addRow(rs, record, isFirstRecord);
                if(isFirstRecord){
                    String url = conn.getMetaData().getURL();
                    if (url.equals("jdbc:columnlist:connection")) {
                        return rs;
                    }
                    isFirstRecord = false;
                }
            }
        } finally {
            reader.close();
        }
        return rs;
    }

    protected static void addRow(SimpleResultSet rs, String record, boolean addColumn) {
        List<Object> row = new ArrayList<Object>();
        StringTokenizer rt = new StringTokenizer(record, "\t");
        while (rt.hasMoreTokens()) {
            String field = rt.nextToken();
            String[] labelValue = parseField(field);
            if (addColumn) {
                rs.addColumn(labelValue[0].toUpperCase(), Types.VARCHAR, Integer.MAX_VALUE, 0);
            }
            row.add(labelValue[1]);
        }
        rs.addRow(row.toArray());
    }

    protected static String[] parseField(String field) {
        if (field == null || field.length() == 0) {
            return null;
        }
        int pos = field.indexOf(':');
        if (pos < 0) {
            return new String[] { field, "" };
        }
        return new String[] { field.substring(0, pos), field.substring(pos + 1) };
    }
}
