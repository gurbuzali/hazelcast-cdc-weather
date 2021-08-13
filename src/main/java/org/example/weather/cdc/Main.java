package org.example.weather.cdc;

import org.example.weather.cdc.handler.QueryHandler;
import org.example.weather.cdc.service.HazelcastService;
import ratpack.server.RatpackServer;

import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) throws Exception {
        RatpackServer.start(server -> server
                .registryOf(r -> r.add(new HazelcastService()))
                .handlers(
                        chain -> chain
                                .get(ctx -> ctx.render(Paths.get("src/main/resources/index.html")))
                                .path("query", new QueryHandler())
                )
        );
    }
}
