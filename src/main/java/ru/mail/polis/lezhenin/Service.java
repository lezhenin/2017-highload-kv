package ru.mail.polis.lezhenin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;

public class Service implements KVService {

    private final static String HTTP_METHOD_PUT = "PUT";
    private final static String HTTP_METHOD_GET = "GET";
    private final static String HTTP_METHOD_DELETE = "DELETE";

    private final static int HTTP_CODE_OK = 200;
    private final static int HTTP_CODE_CREATED = 201;
    private final static int HTTP_CODE_ACCEPTED = 202;
    private final static int HTTP_CODE_BAD_REQUEST = 400;
    private final static int HTTP_CODE_NOT_FOUND = 404;
    private final static int HTTP_CODE_METHOD_NOT_ALLOWED = 405;
    private final static int HTTP_CODE_GONE = 410;
    private final static int HTTP_CODE_GATEWAY_TIMEOUT = 504;

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
        httpServer.stop(0);
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
            sendResponse(httpExchange, HTTP_CODE_NOT_FOUND, null);
            return;
        }

        if (id.isEmpty()) {
            sendResponse(httpExchange, HTTP_CODE_BAD_REQUEST, null);
            return;
        }

        String tempStr = getParameter(httpExchange, "sendData");
        sendData = tempStr != null && tempStr.matches("(?i)true");
        System.out.println("SEND DATA " + sendData + " FROM STRING " + tempStr);

        byte[] byteData = null;
        int resultCode;
        String method = httpExchange.getRequestMethod();

        try {

            switch (method) {

                case HTTP_METHOD_PUT:
                    InputStream requestStream = httpExchange.getRequestBody();
                    byteData = readData(requestStream);
                    storage.putData(id, byteData);
                    System.out.println("P LENGTH " + byteData.length);
                    resultCode = HTTP_CODE_CREATED;
                    byteData = null;
                    break;

                case HTTP_METHOD_GET:
                    if (storage.isDeleted(id)) {
                        resultCode = HTTP_CODE_GONE;
                    } else if (storage.isExist(id)) {
                        if (sendData) {
                            byteData = storage.getData(id);
                            System.out.println("G LENGTH " + byteData.length);
                        }
                        resultCode = HTTP_CODE_OK;
                    } else {
                        resultCode = HTTP_CODE_NOT_FOUND;
                    }
                    break;

                case HTTP_METHOD_DELETE:
                    storage.deleteData(id);
                    resultCode = HTTP_CODE_ACCEPTED;
                    break;

                default:
                    resultCode = HTTP_CODE_METHOD_NOT_ALLOWED;
                    break;
            }

            sendResponse(httpExchange, resultCode, byteData);

        } catch (Exception e) {
            System.out.println("INNER CATCH");
            sendResponse(httpExchange, HTTP_CODE_NOT_FOUND, null);
        }
    }

    private void handleEntityRequest(@NotNull HttpExchange httpExchange) throws IOException {

        System.out.println("\nENTITY");
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

        int answers = 0;
        int hash = id.hashCode();

        String url;
        String parameters = "?id=" + id;

        System.out.println("ID " + id);

        List<Integer> responseCodes = new ArrayList<>(from);

        int actualCode;
        byte [] data = null;
        List<Byte> dataList = null;

        String method = httpExchange.getRequestMethod();

        for (int i = 0; i < from && answers < ask; i++) {

            url = topology.get((Math.abs(hash) + i) % from) + "/v0/inner";
            System.out.println("INDEX " + (Math.abs(hash) + i) % from);
            System.out.println("URL " + url);
            System.out.println("METHOD " + method);

            switch (method) {

                case HTTP_METHOD_PUT:
                    if (data == null) {
                        data = readData(httpExchange.getRequestBody());
                    }
                    dataList = byteArrayToList(data);
                    actualCode = sendRequest(url, parameters, method, dataList, null);
                    responseCodes.add(actualCode);
                    break;

                case HTTP_METHOD_GET:
                    dataList = new ArrayList<>();
                    if (data == null) {
                        actualCode = sendRequest(url, parameters + "&sendData=true", method, null, dataList);
                        if (actualCode == HTTP_CODE_OK) {
                            System.out.println("DATA SAVED");
                            data = byteListToArray(dataList);
                        }
                    } else {
                        actualCode = sendRequest(url, parameters, method);
                    }
                    responseCodes.add(actualCode);
                    break;

                case HTTP_METHOD_DELETE:
                    actualCode = sendRequest(url, parameters, method);
                    responseCodes.add(actualCode);
                    break;

                default:
                    actualCode = HTTP_CODE_METHOD_NOT_ALLOWED;
                    break;
            }

            if (actualCode != HTTP_CODE_GATEWAY_TIMEOUT) {
                answers++;
            }
        }

        if (!method.equals(HTTP_METHOD_GET)) {
            data = null;
        }

        int resultCode = interpretResponses(method, responseCodes, ask, from);

        System.out.println("RESULT CODE " + resultCode);
        sendResponse(httpExchange, resultCode, data);
    }

    private int interpretResponses(String method, List<Integer> responses, int ask, int from) {
        if (Collections.frequency(responses, HTTP_CODE_GATEWAY_TIMEOUT) > from - ask) {
            return HTTP_CODE_GATEWAY_TIMEOUT;
        }
        switch (method) {
            case HTTP_METHOD_PUT:
                if (Collections.frequency(responses, HTTP_CODE_CREATED) >= ask) {
                    return HTTP_CODE_CREATED;
                } else {
                    return HTTP_CODE_NOT_FOUND;
                }
            case HTTP_METHOD_GET:
                if (responses.contains(HTTP_CODE_GONE)) {
                    return HTTP_CODE_NOT_FOUND;
                } else if (responses.contains(HTTP_CODE_OK)) {
                    return HTTP_CODE_OK;
                } else {
                    return HTTP_CODE_NOT_FOUND;
                }
            case HTTP_METHOD_DELETE:
                if (Collections.frequency(responses, HTTP_CODE_ACCEPTED) >= ask) {
                    return HTTP_CODE_ACCEPTED;
                } else {
                    return HTTP_CODE_NOT_FOUND;
                }
            default:
                return HTTP_CODE_METHOD_NOT_ALLOWED;
        }
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

        int code = 504;

        HttpURLConnection connection;
        URL url = new URL(urlString + params);

        connection = (HttpURLConnection) url.openConnection();
        System.out.println("OPENED");
        connection.setRequestMethod(method);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setUseCaches(false);

        connection.setDoInput(true);//todo
        connection.setDoOutput(true);

        try {

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
                InputStream inputStream = connection.getInputStream();
                inputData.addAll(byteArrayToList(readData(inputStream)));
            }

        } catch (IOException ignored) { }

        try {
            code = connection.getResponseCode();
        } catch (IOException ignored) { }

        System.out.println("CODE RECEIVED");
        connection.disconnect();
        System.out.println("CODE " + code);
        return code;
    }
}
