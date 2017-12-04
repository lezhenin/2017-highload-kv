math.randomseed(os.time())

method = "GET"
id_base = "23item"
path = "/v0/entity"

ask = 2
from = 3

counter = 0

request = function()
    counter = counter + 1;
    params = "?" .. "id=" .. id_base .. counter .. "&" .. "replicas=" .. ask .. "/" .. from
    return wrk.format(method, path .. params, nil, nil)
end
