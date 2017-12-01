package ru.mail.polis.lezhenin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static ru.mail.polis.lezhenin.ServiceUtils.*;

public class Service implements KVService {

    private final Executor statusExecutor = Executors.newCachedThreadPool();
    private final Executor entityExecutor = Executors.newCachedThreadPool();
    private final Executor innerExecutor = Executors.newSingleThreadExecutor();

    private static final String ID_KEY = "id";
    private static final String REPLICAS_KEY = "replicas";
    private static final String SEND_DATA_KEY = "sendData";

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
    private final static int HTTP_CODE_INTERNAL_ERROR = 500;

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
                httpExchange -> statusExecutor.execute(() -> {
                    try {
                        sendResponse(httpExchange, HTTP_CODE_OK, null);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        httpExchange.close();
                    }
                })
        );

        httpServer.createContext("/v0/entity",
                (HttpExchange httpExchange) -> entityExecutor.execute(() -> {
                    try {
                        handleEntityRequest(httpExchange);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        httpExchange.close();
                    }
                })
        );

        httpServer.createContext("/v0/inner_interact",
                (HttpExchange httpExchange) -> innerExecutor.execute(() -> {
                    try {
                        handleInnerRequest(httpExchange);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        httpExchange.close();
                    }
                })
        );

    }

    private void handleInnerRequest(@NotNull HttpExchange httpExchange) throws IOException {

        String id = getParameter(httpExchange, ID_KEY);
        int idCode = checkId(id);
        if (idCode != HTTP_CODE_OK) {
            sendResponse(httpExchange, idCode, null);
            return;
        }

        String sendDataKeyStr = getParameter(httpExchange, SEND_DATA_KEY);
        boolean sendData = sendDataKeyStr != null && sendDataKeyStr.matches("(?i)true");

        byte[] byteData = null;
        int resultCode;
        String method = httpExchange.getRequestMethod();

        try {

            switch (method) {

                case HTTP_METHOD_PUT:
                    InputStream requestStream = httpExchange.getRequestBody();
                    byteData = readData(requestStream);
                    storage.putData(id, byteData);
                    resultCode = HTTP_CODE_CREATED;
                    byteData = null;
                    break;

                case HTTP_METHOD_GET:
                    if (storage.isDeleted(id)) {
                        resultCode = HTTP_CODE_GONE;
                    } else if (storage.isExist(id)) {
                        if (sendData) {
                            byteData = storage.getData(id);
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
            sendResponse(httpExchange, HTTP_CODE_INTERNAL_ERROR, null);
        }
    }

    private void handleEntityRequest(@NotNull HttpExchange httpExchange) throws IOException {

        String id = getParameter(httpExchange, ID_KEY);
        int idCode = checkId(id);
        if (idCode != HTTP_CODE_OK) {
            sendResponse(httpExchange, idCode, null);
            return;
        }

        int ask = quorum;
        int from = topology.size();
        String replicas = getParameter(httpExchange, REPLICAS_KEY);

        if ((replicas != null) && replicas.matches("\\d+/\\d+")) {
            String[] parts = replicas.split("/");
            ask = Integer.valueOf(parts[0]);
            from = Integer.valueOf(parts[1]);
        }

        if (ask <= 0 || ask > from) {
            sendResponse(httpExchange, HTTP_CODE_BAD_REQUEST, null);
            return;
        }

        int answers = 0;
        int hash = id.hashCode();

        String url;
        String parameters = "?" + ID_KEY + "=" + id;

        List<Integer> responseCodes = new ArrayList<>(from);

        int actualCode;
        byte[] putData = null;
        byte[] getData = null;

        String method = httpExchange.getRequestMethod();

        try {

            for (int i = 0; i < from && answers < ask; i++) {

                url = topology.get((Math.abs(hash) + i) % from) + "/v0/inner_interact";

                switch (method) {

                    case HTTP_METHOD_PUT:
                        if (putData == null) {
                            putData = readData(httpExchange.getRequestBody());
                        }
                        actualCode = sendRequest(url, parameters, method, putData).code;
                        responseCodes.add(actualCode);
                        break;

                    case HTTP_METHOD_GET:
                        if (getData == null) {
                            ResponsePair pair = sendRequest(url, parameters + "&" + SEND_DATA_KEY + "=true", method);
                            actualCode = pair.code;
                            if (actualCode == HTTP_CODE_OK) {
                                getData = pair.data;
                            }
                        } else {
                            actualCode = sendRequest(url, parameters, method).code;
                        }
                        responseCodes.add(actualCode);
                        break;

                    case HTTP_METHOD_DELETE:
                        actualCode = sendRequest(url, parameters, method).code;
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

        } catch (IOException e) {
            sendResponse(httpExchange, HTTP_CODE_INTERNAL_ERROR, null);
            return;
        }

        int resultCode = interpretResponses(method, responseCodes, ask, from);
        sendResponse(httpExchange, resultCode, getData);
    }

    private int interpretResponses(@NotNull String method, @NotNull List<Integer> responses, int ask, int from) {

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

    private int checkId(@Nullable String id) {
        if (id == null) {
            return HTTP_CODE_NOT_FOUND;
        } else if (id.isEmpty()) {
            return HTTP_CODE_BAD_REQUEST;
        } else {
            return HTTP_CODE_OK;
        }
    }

}
