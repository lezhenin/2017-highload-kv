package ru.mail.polis.lezhenin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static ru.mail.polis.lezhenin.ServiceUtils.*;

public class Service implements KVService {

    private final Executor statusExecutor = Executors.newCachedThreadPool();
    private final Executor entityExecutor = Executors.newCachedThreadPool();
    private final Executor innerExecutor = Executors.newSingleThreadExecutor();

    private Executor replicasFutureExecutor;

    private static final String PATH_INTERNAL = "/v0/internal";
    private static final String PATH_ENTITY = "/v0/entity";
    private static final String PATH_STATUS = "/v0/status";

    static final String ID_KEY = "id";
    static final String REPLICAS_KEY = "replicas";

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
        this(port, data, Collections.singletonList("http://localhost:" + port));
    }

    public Service(int port, @NotNull File data, @NotNull Collection<String> topology) throws IOException {
        this.storage = new FileAccessProvider(data);
        this.httpServer = HttpServer.create(new InetSocketAddress(port), -1);
        this.topology = topology.stream().distinct().collect(Collectors.toList());
        this.quorum = topology.size() / 2 + 1;
        this.replicasFutureExecutor = Executors.newFixedThreadPool(topology.size());
        createContext();
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

        httpServer.createContext(PATH_STATUS,
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

        httpServer.createContext(PATH_ENTITY,
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

        httpServer.createContext(PATH_INTERNAL,
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

        QueryParameters parameters = new QueryParameters(httpExchange);

        String id = parameters.getId();

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
                        byteData = storage.getData(id);
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

        QueryParameters parameters = new QueryParameters(httpExchange);

        String id = parameters.getId();

        if (id == null) {
            sendResponse(httpExchange, HTTP_CODE_NOT_FOUND, null);
            return;
        } else if (id.isEmpty()) {
            sendResponse(httpExchange, HTTP_CODE_BAD_REQUEST, null);
            return;
        }

        int ask, from;
        if (parameters.hasReplicasParameters()) {
            ask = parameters.getAsk();
            from = parameters.getFrom();
        } else {
            ask = quorum;
            from = topology.size();
        }

        if (ask <= 0 || ask > from) {
            sendResponse(httpExchange, HTTP_CODE_BAD_REQUEST, null);
            return;
        }

        int hash = id.hashCode();

        String url;
        String paramStr = "?" + ID_KEY + "=" + id;

        List<FutureTask<ResponsePair>> futureTasks = new ArrayList<>(from);
        List<Integer> responseCodes = new ArrayList<>(from);

        String method = httpExchange.getRequestMethod();

        byte[] putData = method.equals(HTTP_METHOD_PUT) ? readData(httpExchange.getRequestBody()) : null;
        byte[] getData = null;

        for (int i = 0; i < from; i++) {

            url = topology.get((Math.abs(hash) + i) % from) + PATH_INTERNAL;
            FutureTask<ResponsePair> task = null;

            switch (method) {

                case HTTP_METHOD_PUT:
                    task = new FutureTask<>(callableRequest(
                            url, paramStr, method, putData, false));
                    break;

                case HTTP_METHOD_GET:
                    task = new FutureTask<>(callableRequest(
                            url, paramStr, method, null, true));
                    break;

                case HTTP_METHOD_DELETE:
                    task = new FutureTask<>(callableRequest(
                            url, paramStr, method, null, false));
                    break;

                default:
                    responseCodes.add(HTTP_CODE_METHOD_NOT_ALLOWED);
                    break;
            }

            if (task != null) {
                replicasFutureExecutor.execute(task);
                futureTasks.add(task);
            }

        }

        int answers = 0;
        ResponsePair result;

        for (FutureTask<ResponsePair> task : futureTasks) {
            try {
                result = task.<ResponsePair>get();
                responseCodes.add(result.getCode());
                if (result.getCode() != HTTP_CODE_GATEWAY_TIMEOUT) {
                    answers++;
                }
                if (method.equals(HTTP_METHOD_GET) && result.getCode() == HTTP_CODE_OK && getData == null) {
                    getData = result.getData();
                }
            } catch (InterruptedException | ExecutionException e) {
                responseCodes.add(HTTP_CODE_INTERNAL_ERROR);
            }

            if (answers >= ask) {
                break;
            }
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

}
