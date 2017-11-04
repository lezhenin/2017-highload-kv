package ru.mail.polis.lezhenin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Service implements KVService {

    private final static String HTTP_METHOD_PUT = "PUT";
    private final static String HTTP_METHOD_GET = "GET";
    private final static String HTTP_METHOD_DELETE = "DELETE";

    private List<String> topology;
    private int quorum = 0;

    private final FileAccessProvider storage;
    private final HttpServer httpServer;

    public Service(int port, @NotNull File data) throws IOException {
        storage = new FileAccessProvider(data);
        httpServer = HttpServer.create(new InetSocketAddress(port), -1);
        createContext();
    }

    public Service(int port, @NotNull File data, @NotNull Set<String> topology) throws IOException {
        this(port, data);
        this.topology = new ArrayList<>(topology);
        this.quorum = topology.size() / 2 + 1;
    }

    static byte [] byteListToArray(List<Byte> list) {
        byte [] array = new byte[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    static List<Byte> byteArrayToList(byte [] array) {
        List<Byte> list = new ArrayList<>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(i, array[i]);
        }
        return list;
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
                httpExchange -> {
                    System.out.println("STATUS ASKED");
                    sendResponse(httpExchange, 200, null);
                });


        httpServer.createContext("/v0/entity",
                this::handleEntityRequest
        );

        httpServer.createContext("/v0/inner",
                this::handleInnerRequest
        );

        httpServer.setExecutor(Executors.newFixedThreadPool(3));
    }

    private void handleInnerRequest(@NotNull HttpExchange httpExchange) throws IOException {
        String id;
        boolean sendData;

        System.out.println("INNER");

        id = getParameter(httpExchange, "id");

        if (id == null) {
            sendResponse(httpExchange, 404, null);
            return;
        }

        if (id.isEmpty()) {
            sendResponse(httpExchange, 400, null);
            return;
        }

        String tempStr = getParameter(httpExchange, "sendData");
        sendData = tempStr != null && tempStr.matches("(?i)true");
        System.out.println("SEND DATA " + sendData + " FROM STRING " + tempStr);
//        sendData = true; // temp

        try {

            byte[] byteData;
            String method = httpExchange.getRequestMethod();
            switch (method) {

                case HTTP_METHOD_PUT:
                    InputStream requestStream = httpExchange.getRequestBody();
                    byteData = readData(requestStream);
                    storage.putData(id, byteData);
                    System.out.println("P LENGTH " + byteData.length);
                    sendResponse(httpExchange, 201, null);
                    break;

                case HTTP_METHOD_GET:
                    byteData = storage.getData(id);
                    if (sendData) {
                        sendResponse(httpExchange, 200, byteData);
                        System.out.println("G LENGTH " + byteData.length);
                    } else {
                        sendResponse(httpExchange, 200, null);
                    }
                    break;

                case HTTP_METHOD_DELETE:
                    storage.deleteData(id);
                    sendResponse(httpExchange, 202, null);
                    break;
            }

        } catch (Exception e) {
            System.out.println("INNER CATCH");
            sendResponse(httpExchange, 404, null);
        }
    }

    private void handleEntityRequest(@NotNull HttpExchange httpExchange) throws IOException {

        System.out.println("ENTITY");
        String id;

        id = getParameter(httpExchange, "id");

        if (id == null) {
            sendResponse(httpExchange, 404, null);
            return;
        }

        if (id.isEmpty()) {
            sendResponse(httpExchange, 400, null);
            return;
        }

        int ask = quorum;
        int from = topology.size();
        String replicas = getParameter(httpExchange, "replicas");

        if ((replicas != null) && replicas.matches("\\d+/\\d+")) {
            String[] parts = replicas.split("/");
            ask = Integer.valueOf(parts[0]);
            from = Integer.valueOf(parts[1]);
        }

        System.out.println("ASK " + ask);
        System.out.println("FROM " + from);

        if (ask <= 0 || ask > from) {
            sendResponse(httpExchange, 400, null);
            return;
        }
        //todo checks

        int answers = 0;
        int hash = id.hashCode();

        String url;
        String parameters = "?id=" + id;

        System.out.println("ID " + id);

        int actualCode = 504;
        List<String> available = new ArrayList<>();

        for (int i = 0; i < topology.size() && available.size() < from; i++) {
            System.out.println("HASH" + hash + "  INDX " + (Math.abs(hash) + i) % topology.size());
            url = topology.get((Math.abs(hash) + i) % topology.size());
            System.out.println("ASK STATUS " + url);
            int code = sendRequest(url + "/v0/status", "", HTTP_METHOD_GET);
            if (code == 200) {
                available.add(url);
            }
        }

        if (available.size() < ask) {
            sendResponse(httpExchange, 504, null);
            return;
        }

        byte [] data = new byte[0];

        for (int i = 0; i < available.size() && answers < ask; i++) {
            url = available.get(i) + "/v0/inner";
            System.out.println("URL " + url);
            System.out.println(httpExchange.getLocalAddress().getHostName());
            String method = httpExchange.getRequestMethod();
            System.out.println("METHOD " + method);

            if (method.equals(HTTP_METHOD_PUT) && i == 0) {
                data = readData(httpExchange.getRequestBody());
            }

            switch (method) {

                case HTTP_METHOD_PUT:
                    List<Byte> dataList = byteArrayToList(data);
                    try {
                        actualCode = sendRequest(url, parameters, method, dataList, null);
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                    }
                    if (actualCode == 201) {
                        answers++;
                    }
                    break;

                case HTTP_METHOD_GET:
                    if (answers != ask-1) {
                        actualCode = sendRequest(url, parameters, method);
                    } else {
                        List<Byte> dataList2 = new ArrayList<>();
                        actualCode = sendRequest(url, parameters + "&sendData=true", method, null, dataList2);
                        if (actualCode == 200) {
                            byte [] data2 = byteListToArray(dataList2);
                            sendResponse(httpExchange, 200, data2);
                            return;
                        }
                    }
                    if (actualCode == 404) {
                        System.out.println("INNER GET ERROR");
                        sendResponse(httpExchange, 404, null);
                        return;
                    }
                    if (actualCode == 200) {
                        answers++;
                    }
                    break;

                case HTTP_METHOD_DELETE:
                    actualCode = sendRequest(url, parameters, method);
                    if (actualCode == 202) {
                        answers++;
                    }
                    break;
            }
        }

        System.out.println("ACTUAL CODE " + actualCode);
        sendResponse(httpExchange, actualCode, null);
        System.out.println("ACTUAL CODE " + actualCode);

    }

    private void sendResponse(HttpExchange exchange, int code, @Nullable byte[] data) throws IOException {
        if (data != null) {
            exchange.sendResponseHeaders(code, data.length);
            OutputStream responseStream = exchange.getResponseBody();
            responseStream.write(data);
            responseStream.flush();
        }
        else {
            exchange.sendResponseHeaders(code, 0);
        }
        exchange.close();
        System.out.println("CODE " + code + " SENT");
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

    @Nullable
    private String getParameter(@NotNull HttpExchange httpExchange, @NotNull String key)
            throws UnsupportedEncodingException {
        String query = httpExchange.getRequestURI().getQuery();
        query = URLDecoder.decode(query, "UTF-8");
        Map<String, String> params = parseQuery(query);
        return params.get(key);
    }

    @NotNull
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

    private int sendRequest(@NotNull String urlString, @NotNull String params,
                           @NotNull String method) throws IOException {
        return sendRequest(urlString, params, method, null, null);
    }

    private int sendRequest(@NotNull String urlString, @NotNull String params, @NotNull String method,
                            @Nullable List<Byte> outputData,
                            @Nullable List<Byte> inputData) throws IOException {

        HttpURLConnection connection;
        URL url = new URL(urlString + params);

        connection = (HttpURLConnection) url.openConnection();
        System.out.println("OPENED");
        connection.setRequestMethod(method);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setUseCaches(false);

        connection.setDoInput(true);//todo
        connection.setDoOutput(true);

        if (outputData != null) {
            System.out.println("WRITE OUT");
            connection.setRequestProperty("Content-Length", String.valueOf(outputData.size()));
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(byteListToArray(outputData));
            outputStream.close();
        }

        if (inputData != null) {
            System.out.println("WRITE IN");
            inputData.clear();
            try {
                InputStream inputStream = connection.getInputStream();
                inputData.addAll(byteArrayToList(readData(inputStream)));
            } catch (IOException ignored) { }
        }

        int code = 504;
        try {
            code = connection.getResponseCode();
        } catch (IOException ignored) { }
        System.out.println("CODE RECEIVED");
        connection.disconnect();
        System.out.println("CODE " + code);
        return code;
    }
}
