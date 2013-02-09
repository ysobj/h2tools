package com.karatebancho.h2tools;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import org.junit.Test;

public class LTSVTest {

    @Test
    public void test() throws Exception {
        DriverManager.registerDriver(new org.h2.Driver());
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
        conn.createStatement().execute("create alias ltsvread for \"com.karatebancho.h2tools.LTSV.read\"");
        ResultSet rs = conn.createStatement().executeQuery("select * from ltsvread('classpath:test.log');");
        rs.next();
        assertEquals("frank", rs.getString("user"));
        assertEquals("http://www.example.com/start.html", rs.getString("REFERER"));
        rs.close();

    }

}
