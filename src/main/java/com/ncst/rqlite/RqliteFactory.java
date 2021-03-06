package com.ncst.rqlite;


public enum RqliteFactory {
    INSTANCE;

    /**
     * Create a connection to a rqlite node.
     *
     * @param proto
     *            the protocol, either "http" or "https"
     * @param host
     *            the host name of the rqlite note
     * @param port
     *            the port on the rqlite node
     * @return a rqlite client instance.
     */
    public static Rqlite connect(final String proto, final String host, final Integer port) {
        return new RqliteImpl(proto, host, port);
    }

    public static Rqlite connect(final String config) {
        return new RqliteImpl(config);
    }
}
