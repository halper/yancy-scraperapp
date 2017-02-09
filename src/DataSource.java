import org.apache.commons.dbcp2.BasicDataSource;

/**
 * Created by alper on 1/19/17.
 */
public class DataSource {
    private static final int CONN_POOL_SIZE = 5;

    private BasicDataSource bds = new BasicDataSource();

    private DataSource() {
        MySQLProperties mProps = new MySQLProperties();
        //Set database driver name
        bds.setDriverClassName(mProps.getProperty("MYSQL_DB_DRIVER_CLASS_NAME"));
        //Set database url
        bds.setUrl(mProps.getProperty("MYSQL_DB_URL"));
        //Set database user
        bds.setUsername(mProps.getProperty("MYSQL_DB_USERNAME"));
        //Set database password
        bds.setPassword(mProps.getProperty("MYSQL_DB_PASSWORD"));
        //Set the connection pool size
        bds.setInitialSize(CONN_POOL_SIZE);
    }

    private static class DataSourceHolder {
        private static final DataSource INSTANCE = new DataSource();
    }

    public static DataSource getInstance() {
        return DataSourceHolder.INSTANCE;
    }

    public BasicDataSource getBds() {
        return bds;
    }

    public void setBds(BasicDataSource bds) {
        this.bds = bds;
    }
}
