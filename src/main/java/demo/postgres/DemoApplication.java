package demo.postgres;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class DemoApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = createConnectionFactory();
        DatabaseClient client = createDatabaseClient(connectionFactory);
        ReactiveTransactionManager tm = createTransactionManager(connectionFactory);

        client.execute("create table foo (id serial primary key, value char(1) not null)")
                .fetch().rowsUpdated().block();

        TransactionalOperator operator = TransactionalOperator.create(tm);

//        LOGGER.info("rawInsertString inserted {} rows", rawInsertString(client, operator, 5000).size());
//        LOGGER.info("insertWithBind inserted {} rows", insertWithBind(client, operator, 5000).size());
//        LOGGER.info("rawInsertStringWithReturning inserted {} rows", rawInsertStringWithReturning(client, operator, 5000).size());
        LOGGER.info("insertWithBindAndReturning inserted {} rows", insertWithBindAndReturning(client, operator).size());

//        behaviorChangingSample(client, operator, 1);
    }

    private static List<String> insertWithBind(DatabaseClient client, TransactionalOperator operator, long rowsToInsert) {
        return client.execute("INSERT INTO foo(value) VALUES (:value)")
                .bind("value", "x")
                .fetch().first()
                .repeat(rowsToInsert - 1).then()
                .as(operator::transactional)
                .then(client.execute("select * from foo")
                        .map((row, rowMetadata) -> String.valueOf(row.get("id")))
                        .all().collectList())
                .defaultIfEmpty(new ArrayList<>())
                .block();
    }

    private static List<String> rawInsertString(DatabaseClient client, TransactionalOperator operator, long rowsToInsert) {
        return client.execute("INSERT INTO foo(value) VALUES ('x')")
                .fetch().first()
                .repeat(rowsToInsert - 1).then()
                .as(operator::transactional)
                .then(client.execute("select * from foo")
                        .map((row, rowMetadata) -> String.valueOf(row.get("id")))
                        .all().collectList())
                .defaultIfEmpty(new ArrayList<>())
                .block();
    }

    private static List<String> rawInsertStringWithReturning(DatabaseClient client, TransactionalOperator operator, long rowsToInsert) {
        return client.execute("INSERT INTO foo(value) VALUES ('x') RETURNING *")
                .map((row, metadata) -> {
                    LOGGER.info("rawInsertStringWithReturning Id: {}", row.get("id"));
                    return row;
                }).first()
                .repeat(rowsToInsert - 1).then()
                .as(operator::transactional)
                .then(client.execute("select * from foo")
                        .map((row, rowMetadata) -> String.valueOf(row.get("id")))
                        .all().collectList())
                .defaultIfEmpty(new ArrayList<>())
                .block();
    }

    private static List<String> insertWithBindAndReturning(DatabaseClient client, TransactionalOperator operator) {
        return client.execute("INSERT INTO foo(value) VALUES (:value) RETURNING *")
                .bind("value", "x")
                .map((row, metadata) -> {
                    LOGGER.info("insertWithBindAndReturning: {}", row.get("id"));
                    return row;
                }).first()
                .repeat(5000 - 1).then()
                .as(operator::transactional)
                .then(client.execute("select * from foo")
                        .map((row, rowMetadata) -> String.valueOf(row.get("id")))
                        .all().collectList())
                .defaultIfEmpty(new ArrayList<>())
                .block();
    }

    private static void behaviorChangingSample(DatabaseClient client, TransactionalOperator operator, long rowsToInsert) {
        LOGGER.info("rawInsertString inserted {} rows", insertWithBind(client, operator, rowsToInsert).size());
        LOGGER.info("insertWithBindAndReturning inserted {} rows", insertWithBindAndReturning(client, operator).size());
    }

    private static PostgreSQLContainer<?> createContainer() {
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:12.1-alpine");
        container.start();

        return container;
    }

    private static ConnectionFactory createConnectionFactory() {
        PostgreSQLContainer<?> container = createContainer();

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "pool")
                .option(ConnectionFactoryOptions.PROTOCOL, "postgres")
                .option(ConnectionFactoryOptions.HOST, container.getContainerIpAddress())
                .option(ConnectionFactoryOptions.PORT, container.getMappedPort(5432))
                .option(ConnectionFactoryOptions.USER, container.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, container.getPassword())
                .option(ConnectionFactoryOptions.DATABASE, container.getDatabaseName())
                .build());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxIdleTime(Duration.ofMinutes(10))
                .maxLifeTime(Duration.ofMinutes(30))
                .initialSize(2)
                .maxSize(5)
                .build();

        return new ConnectionPool(configuration);
    }

    private static DatabaseClient createDatabaseClient(ConnectionFactory connectionFactory) {
        return DatabaseClient.create(connectionFactory);
    }

    private static ReactiveTransactionManager createTransactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }
}