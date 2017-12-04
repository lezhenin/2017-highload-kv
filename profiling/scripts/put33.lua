math.randomseed(os.time())

method = "PUT"
id_base = "33item"
path = "/v0/entity"
body = "some data in body..."
for i=1,math.random(200, 300) do
    body = body .. math.random(1000, 9999)
end

ask = 3
from = 3

counter = 0

request = function()
    counter = counter + 1;
    params = "?" .. "id=" .. id_base .. counter .. "&" .. "replicas=" .. ask .. "/" .. from
    return wrk.format(method, path .. params, nil, body)
end
