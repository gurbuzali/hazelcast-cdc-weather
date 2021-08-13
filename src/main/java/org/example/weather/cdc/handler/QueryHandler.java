package org.example.weather.cdc.handler;

import org.example.weather.cdc.model.WeatherData;
import org.example.weather.cdc.service.HazelcastService;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.http.Status;
import ratpack.jackson.Jackson;
import ratpack.util.MultiValueMap;

public class QueryHandler implements Handler {
    @Override
    public void handle(Context ctx) throws Exception {
        MultiValueMap<String, String> map = ctx.getRequest().getQueryParams();
        String city = map.get("city");
        HazelcastService hazelcastService = ctx.get(HazelcastService.class);
        WeatherData weatherData = hazelcastService.map().get(city);
        if (weatherData == null) {
            ctx.getResponse().status(Status.BAD_REQUEST);
            ctx.render(Jackson.json("No city found"));
        } else {
            ctx.render(Jackson.json(weatherData));
        }
    }
}
