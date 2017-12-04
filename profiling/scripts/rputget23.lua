math.randomseed(os.time())

id_base = "rpg23item"
path = "/v0/entity"
body_data = "some data in body..."
for i=1,math.random(200, 300) do
    body_data = body_data .. math.random(1000, 9999)
end

id_num = 100

ask = 2
from = 3

counter = 0

request = function()

    counter = counter + 1;

    if (counter % 2 == 0) then
        method = "GET"
        body = nil
    else
        method = "PUT"
        body = body_data
    end

    id = id_base .. math.random(id_num)
    params = "?" .. "id=" .. id .. "&" .. "replicas=" .. ask .. "/" .. from
    return wrk.format(method, path .. params, nil, body)
end

