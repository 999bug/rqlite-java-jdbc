package com.ncst.rqlite;

import com.ncst.rqlite.dto.Pong;
import com.ncst.rqlite.Rqlite;
import com.ncst.rqlite.RqliteFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class RqliteFactoryTest {

    @Test
    public void testCreateRqliteInstance() {
        Rqlite rqlite = RqliteFactory.connect("http", "localhost", 4101);
        Assert.assertNotNull(rqlite);
    }

    @Test
    public void testCreateRqliteInstancePing() {
        Rqlite rqlite = RqliteFactory.connect("http", "localhost", 4101);
        Pong pong = rqlite.Ping();
        Assert.assertEquals(getRqliteVersion(), pong.version);
    }

    private String getRqliteVersion() {
        Map<String, String> getenv = System.getenv();
        if (getenv.containsKey("RQLITE_VERSION")) {
            return getenv.get("RQLITE_VERSION");
        }
        return "unknown";
    }
}
