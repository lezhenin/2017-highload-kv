math.randomseed(os.time())

id_base = "pg33item"
path = "/v0/entity"
body_data = "some data in body..."
for i=1,math.random(200, 300) do
    body_data = body_data .. math.random(1000, 9999)
end

ask = 3
from = 3

counter = 0

request = function()
    counter = counter + 1;
    if (counter % 2 == 0) then
        method = "GET"
        id = id_base .. (counter - 1)
        body = nil
    else
        method = "PUT"
        id = id_base .. counter
        body = body_data

    end
    params = "?" .. "id=" .. id .. "&" .. "replicas=" .. ask .. "/" .. from
    return wrk.format(method, path .. params, nil, body)
end
