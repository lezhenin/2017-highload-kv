math.randomseed(os.time())

method = "GET"
id_base = "r33item"
path = "/v0/entity"

ask = 3
from = 3

id_num = 100


math.randomseed(os.time())

request = function()
    id = id_base .. math.random(id_num)
    params = "?" .. "id=" .. id .. "&" .. "replicas=" .. ask .. "/" .. from
    return wrk.format(method, path .. params, nil, nil)
end

