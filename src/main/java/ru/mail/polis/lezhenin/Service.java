package ru.mail.polis.lezhenin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class Service implements KVService {

    private final static String HTTP_METHOD_PUT = "PUT";
    private final static String HTTP_METHOD_GET = "GET";
    private final static String HTTP_METHOD_DELETE = "DELETE";

    private final FileAccessProvider storage;
    private final HttpServer httpServer;

    public Service(int port, @NotNull File data) throws IOException {
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
                this::handleEntityRequest
        );
    }

    private void handleEntityRequest(@NotNull HttpExchange httpExchange) throws IOException {

        String id;

        try {
            id = getId(httpExchange);
        } catch (IllegalArgumentException | UnsupportedEncodingException e) {
            httpExchange.sendResponseHeaders(400, 0);
            httpExchange.close();
            return;
        }

        try {

            byte[] byteData;
            String method = httpExchange.getRequestMethod();
            switch (method) {

                case HTTP_METHOD_PUT:
                    InputStream requestStream = httpExchange.getRequestBody();
                    byteData = readData(requestStream);
                    storage.putData(id, byteData);
                    httpExchange.sendResponseHeaders(201, 0);
                    break;

                case HTTP_METHOD_GET:
                    byteData = storage.getData(id);
                    httpExchange.sendResponseHeaders(200, byteData.length);
                    OutputStream responseStream = httpExchange.getResponseBody();
                    responseStream.write(byteData);
                    break;

                case HTTP_METHOD_DELETE:
                    storage.deleteData(id);
                    httpExchange.sendResponseHeaders(202, 0);
                    break;
            }

        } catch (Exception e) {
            httpExchange.sendResponseHeaders(404, 0);
        }

        httpExchange.close();
    }


    private byte[] readData(@NotNull InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            for (int len; (len = inputStream.read(buffer)) != -1; ) {
                outputStream.write(buffer, 0, len);
            }
            outputStream.flush();
            return outputStream.toByteArray();
        }
    }

    @NotNull
    private String getId(@NotNull HttpExchange httpExchange)
            throws IllegalArgumentException, UnsupportedEncodingException {
        String query = httpExchange.getRequestURI().getQuery();
        query = URLDecoder.decode(query, "UTF-8");
        Map<String, String> params = parseQuery(query);
        if (!params.containsKey("id") || params.get("id").isEmpty()) {
            throw new IllegalArgumentException("ID is empty.");
        }
        return params.get("id");
    }

    private Map<String, String> parseQuery(@NotNull String query) {
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
