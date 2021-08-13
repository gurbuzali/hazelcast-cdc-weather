package org.example.weather.cdc.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapLoaderLifecycleSupport;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class CdcMapLoader<K, V> implements MapLoader<K, V>, MapLoaderLifecycleSupport, Serializable {

    private transient HazelcastInstance hz;
    private String mapName;

    private String url;
    private String host;
    private int port;
    private String cluster;
    private String username;
    private String password;
    private String database;
    private String table;
    private String keyName;
    private String statement;
    private final FunctionEx<ResultSet, V> mapFn;
    private final Class<V> clazz;

    private transient Job job;
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    public CdcMapLoader(FunctionEx<ResultSet, V> mapFn, Class<V> clazz) {
        this.mapFn = mapFn;
        this.clazz = clazz;
    }

    @Override
    public V load(K key) {
        System.err.println("Loading key: " + key);
        return uncheckCall(() -> {
            preparedStatement.setString(1, key.toString());
            ResultSet resultSet = preparedStatement.executeQuery();
            if (!resultSet.next()) {
                return null;
            }
            return mapFn.apply(resultSet);
        });
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        HashMap<K, V> map = new HashMap<>();
        keys.forEach(key -> map.put(key, load(key)));
        return map;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        return null;
    }

    protected Connection getSqlConnection() throws SQLException {
        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        properties.put("useSSL", "false");

        return DriverManager.getConnection(url, properties);
    }

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.host = properties.getProperty("host");
        this.port = Integer.parseInt(properties.getProperty("port"));
        this.cluster = properties.getProperty("cluster");
        this.database = properties.getProperty("database");
        this.table = properties.getProperty("table");
        this.username = properties.getProperty("username");
        this.password = properties.getProperty("password");
        this.keyName = properties.getProperty("keyname");

        this.statement = String.format("select * from %s where %s = ?", table, keyName);
        this.url = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
        this.hz = hazelcastInstance;
        this.mapName = mapName;

        uncheckRun(() -> {
            connection = getSqlConnection();
            preparedStatement = connection.prepareStatement(statement);
        });

        startCdcJob();
    }

    private void startCdcJob() {
        Pipeline p = Pipeline.create();

        p.readFrom(source())
                .withoutTimestamps()
                .filter(record -> {
                    Operation operation = record.operation();
                    return operation.equals(Operation.UPDATE) || operation.equals(Operation.DELETE);
                })
                .peek()
                .writeTo(Sinks.mapWithEntryProcessor(mapName,
                        r -> r.key().toMap().get(keyName),
                        r -> new UpdateDeleteEntryProcessor(r.operation(), r.value().toObject(clazz))
                ));

        job = hz.getJet().newJob(p);
    }

    private StreamSource<ChangeRecord> source() {
        return MySqlCdcSources.mysql(table)
                .setDatabaseAddress(host)
                .setDatabasePort(port)
                .setDatabaseUser(username)
                .setDatabasePassword(password)
                .setClusterName(cluster)
                .setReconnectBehavior(RetryStrategies.indefinitely(1000))
                .setDatabaseWhitelist(database)
                .setTableWhitelist(database + "." + table)
                .build();
    }

    class UpdateDeleteEntryProcessor implements EntryProcessor<Object, Object, Void> {

        private final Operation operation;
        private final Object value;

        public UpdateDeleteEntryProcessor(Operation operation, Object value) {
            this.operation = operation;
            this.value = value;
        }

        @Override
        public Void process(Map.Entry entry) {
            if (operation.equals(Operation.DELETE)) {
                entry.setValue(null);
            } else {
                entry.setValue(value);
            }
            return null;
        }
    }

    @Override
    public void destroy() {
        job.cancel();
        if (connection != null) {
            uncheckRun(() -> connection.close());
        }
    }
}
