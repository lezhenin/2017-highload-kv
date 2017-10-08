package ru.mail.polis.lezhenin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import ru.mail.polis.KVService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class Service implements KVService {

    private final FileAccessProvider storage;
    private final HttpServer httpServer;

    public Service(int port, File data) throws IOException {
        storage = new FileAccessProvider(data);
        httpServer = HttpServer.create(new InetSocketAddress(port), -1);
        createContext();
    }

    @Override
    public void start() {
        httpServer.start();
    }

    @Override
    public void stop() {
        httpServer.stop(1);
    }

    private void createContext() {

        httpServer.createContext("/v0/status",
                httpExchange -> httpExchange.sendResponseHeaders(200, -1));

        httpServer.createContext("/v0/entity",
                httpExchange -> {

                    Map<String, String> params = parseQuery(httpExchange.getRequestURI().getQuery());
                    if (!params.containsKey("id") || params.get("id").isEmpty()) {
                        httpExchange.sendResponseHeaders(400, 0);
                        httpExchange.close();
                        return;
                    }

                    String id = params.get("id");

                    byte [] byteData;
                    String method = httpExchange.getRequestMethod();
                    switch (method) {

                        case "PUT":
                            InputStream requestStream = httpExchange.getRequestBody();
                            int length = requestStream.available();
                            byteData = new byte[length];
                            requestStream.read(byteData);
                            storage.putData(id, byteData);
                            httpExchange.sendResponseHeaders(201, 0);
                            break;

                        case "GET":
                            try {
                                byteData = storage.getData(id);
                                httpExchange.sendResponseHeaders(200, byteData.length);
                                OutputStream responseStream = httpExchange.getResponseBody();
                                responseStream.write(byteData);
                            } catch (NoSuchElementException e) {
                                httpExchange.sendResponseHeaders(404, 0);
                            }
                            break;

                        case "DELETE":
                            storage.deleteData(id);
                            httpExchange.sendResponseHeaders(202, 0);
                            break;
                    }

                    httpExchange.close();
                }
        );
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> result = new HashMap<>();
        for (String param : query.split("&")) {
            String pair[] = param.split("=");
            if (pair.length > 1) {
                result.put(pair[0], pair[1]);
            } else {
                result.put(pair[0], "");
            }
        }
        return result;
    }
}
