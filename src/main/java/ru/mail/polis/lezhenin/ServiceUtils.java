package ru.mail.polis.lezhenin;

import com.sun.net.httpserver.HttpExchange;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static ru.mail.polis.lezhenin.Service.ID_KEY;
import static ru.mail.polis.lezhenin.Service.REPLICAS_KEY;

public final class ServiceUtils {



    public static class QueryParameters {

        private final String id;
        private final int ask;
        private final int from;
        private final boolean hasReplicasParameters;

        public QueryParameters(HttpExchange exchange) throws UnsupportedEncodingException {
            Map<String, String> query = parseQuery(exchange);
            id = query.get(ID_KEY);
            String replicas = query.get(REPLICAS_KEY);
            hasReplicasParameters = (replicas != null) && replicas.matches("\\d+/\\d+");
            if (hasReplicasParameters) {
                String[] parts = replicas.split("/");
                ask = Integer.valueOf(parts[0]);
                from = Integer.valueOf(parts[1]);
            } else {
                ask = 0;
                from = 0;
            }
        }

        public String getId() {
            return id;
        }

        public int getAsk() {
            return ask;
        }

        public int getFrom() {
            return from;
        }

        public boolean hasReplicasParameters() {
            return hasReplicasParameters;
        }
    }

    public static class ResponsePair {

        private final int code;
        private final byte [] data;

        public ResponsePair(int code, @Nullable byte[] data) {
            this.code = code;
            this.data = data;
        }

        public int getCode() {
            return code;
        }

        public byte[] getData() {
            return data;
        }
    }

    public static void sendResponse(@NotNull HttpExchange exchange, int code, @Nullable byte[] data) throws IOException {
        if (data != null) {
            exchange.sendResponseHeaders(code, data.length);
            OutputStream responseStream = exchange.getResponseBody();
            responseStream.write(data);
            responseStream.flush();
        }
        else {
            exchange.sendResponseHeaders(code, 0);
        }
    }

    @NotNull
    public static ResponsePair sendRequest(@NotNull String urlString, @NotNull String params, @NotNull String method,
                            @Nullable byte [] requestData, boolean doInput) throws IOException {

        int code = 504;
        byte [] responseData = null;

        HttpURLConnection connection;
        URL url = new URL(urlString + params);

        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setUseCaches(false);
        connection.setDoInput(true);

        try {

            if (requestData != null) {
                connection.setDoOutput(true);
                OutputStream outputStream = connection.getOutputStream();
                outputStream.write(requestData);
                outputStream.close();
            }

            if (doInput) {
                InputStream inputStream = connection.getInputStream();
                responseData = readData(inputStream);
            }

        } catch (IOException ignored) { }

        try {
            code = connection.getResponseCode();
        } catch (IOException ignored) { }

        connection.disconnect();

        return new ResponsePair(code, responseData);
    }

    public static Callable<ResponsePair> callableRequest(
            @NotNull String urlString, @NotNull String params, @NotNull String method,
            @Nullable byte [] requestData, boolean doInput) {
        return () -> sendRequest(urlString, params, method, requestData, doInput);
    }

    public static byte[] readData(@NotNull InputStream inputStream) throws IOException {
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
    public static Map<String, String> parseQuery(@NotNull HttpExchange httpExchange)
            throws UnsupportedEncodingException {
        String query = httpExchange.getRequestURI().getQuery();
        query = URLDecoder.decode(query, "UTF-8");
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
