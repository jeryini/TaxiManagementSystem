package com.jernejerin.traffic.helper;

import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * A helper class for setting up polling driver for MySQL.
 * </p>
 *
 * @author Jernej Jerin
 */
public class PollingDriver {
    private final static Logger LOGGER = Logger.getLogger(PollingDriver.class.getName());

    /**
     * Setup MySQL driver using passed parameters.
     *
     * @param connectURI Connection string to the MySQL DB.
     * @param userDB Username of the MySQL DB.
     * @param passDB Password of the MySQL DB.
     * @throws Exception
     */
    public static void setupDriver(String connectURI, String userDB, String passDB) throws Exception {
        // First we load the underlying JDBC driver.
        // You need this if you don't use the jdbc.drivers
        // system property.
        LOGGER.log(Level.INFO, "Started setting up driver.");
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory =
                new DriverManagerConnectionFactory(connectURI, userDB, passDB);

        //
        // Next, we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself...
        //
        Class.forName("org.apache.commons.dbcp2.PoolingDriver");
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");

        //
        // ...and register our pool with it.
        //
        driver.registerPool("taxi", connectionPool);
        LOGGER.log(Level.INFO, "Finished setting up driver.");
    }

    /**
     * <p>
     * Get status of the registered pool.
     * </p>
     *
     * @throws Exception
     */
    public static void printDriverStats() throws Exception {
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
        ObjectPool<? extends Connection> connectionPool = driver.getConnectionPool("taxi");

        System.out.println("NumActive: " + connectionPool.getNumActive());
        System.out.println("NumIdle: " + connectionPool.getNumIdle());
    }

    /**
     * <p>
     * Shutdown driver for pool taxi.
     * </p>
     *
     * @throws Exception
     */
    public static void shutdownDriver() throws Exception {
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
        driver.closePool("taxi");
    }
}
