package com.mera.education.grpc.number.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GreetingServer {
    private static Logger logger = LoggerFactory.getLogger(GreetingServer.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        logger.debug("gRPC Server is started");

        Server server = ServerBuilder.forPort(5051)
                .addService(new GreetServiceImpl())
                .build();

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread (()->{
            System.out.println("Received Shutdown Request");
            server.shutdown();
            System.out.println("Successfully stopped the server");
        }));

        server.awaitTermination();
    }
}
