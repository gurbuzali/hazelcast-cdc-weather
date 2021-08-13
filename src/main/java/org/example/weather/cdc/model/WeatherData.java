package org.example.weather.cdc.model;

import java.io.Serializable;

public class WeatherData implements Serializable {

    public String city;
    public String country;
    public String today;
    public String tomorrow;

    public WeatherData() {
    }

    public WeatherData(String cityName, String countryName, String today, String tomorrow) {
        this.city = cityName;
        this.country = countryName;
        this.today = today;
        this.tomorrow = tomorrow;
    }
}
