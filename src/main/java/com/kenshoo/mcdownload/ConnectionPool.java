package com.kenshoo.mcdownload;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.jooq.lambda.Unchecked;

import java.sql.Connection;

public class ConnectionPool {

    private static Supplier<DataSource> lazyDataSource = Suppliers.memoize(ConnectionPool::newTomcatDataSource);

    private static DataSource newTomcatDataSource() {
        DataSource datasource = new DataSource();
        datasource.setPoolProperties(DatabaseProperties.get());
        return datasource;

    }

    public static Connection get() {
        return Unchecked.supplier(() -> lazyDataSource.get().getConnection()).get();
    }
}
