
method = "GET"
path = "/v0/status"

request = function()
    return wrk.format(method, path, nil, nil)
end
