math.randomseed(os.time())

method = "GET"
id_base = "33item"
path = "/v0/entity"

ask = 3
from = 3

counter = 0

request = function()
    counter = counter + 1;
    params = "?" .. "id=" .. id_base .. counter .. "&" .. "replicas=" .. ask .. "/" .. from
    return wrk.format(method, path .. params, nil, nil)
end
