math.randomseed(os.time())

method = "PUT"
id_base = "r23item"
path = "/v0/entity"
body = "some data in body..."
for i=1,math.random(200, 300) do
    body = body .. math.random(1000, 9999)
end

ask = 2
from = 3

id_num = 100

math.randomseed(os.time())

request = function()
    id = id_base .. math.random(id_num)
    params = "?" .. "id=" .. id .. "&" .. "replicas=" .. ask .. "/" .. from
    return wrk.format(method, path .. params, nil, nil)
end
