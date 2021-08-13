package org.example.weather.cdc.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.example.weather.cdc.model.WeatherData;
import ratpack.service.Service;
import ratpack.service.StartEvent;
import ratpack.service.StopEvent;

import java.util.Properties;

public class HazelcastService implements Service {

    private static final String MAP_NAME = "weather";

    HazelcastInstance hz;

    @Override
    public void onStart(StartEvent event) throws Exception {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);

        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();

        Properties properties = mapStoreConfig.getProperties();
        properties.setProperty("host", "localhost");
        properties.setProperty("port", "3306");
        properties.setProperty("cluster", "dbserver1");
        properties.setProperty("database", "weather");
        properties.setProperty("table", "weather_data");
        properties.setProperty("username", "root");
        properties.setProperty("password", "debezium");
        properties.setProperty("keyname", "city");

        mapStoreConfig.setEnabled(true).setImplementation(new CdcMapLoader<String, WeatherData>(
                rs -> {
                    String city = rs.getString(1);
                    String country = rs.getString(2);
                    String today = rs.getString(3);
                    String tomorrow = rs.getString(4);
                    return new WeatherData(city, country, today, tomorrow);
                }, WeatherData.class));

        hz = Hazelcast.newHazelcastInstance(config);
    }

    public IMap<String, WeatherData> map() {
        return hz.getMap(MAP_NAME);
    }

    @Override
    public void onStop(StopEvent event) throws Exception {
        hz.shutdown();
    }

}
