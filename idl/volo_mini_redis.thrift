namespace rs mini.redis

enum RequestType {
    Get,
    Set,
    Del,
    Ping,
    Subscribe,
    Publish,
    Illegal,
    Exit,
}

enum ResponseType {
    Value,
    Ok,
    Trap,
}

struct RedisRequest {
    1: optional list<string> key,
    2: optional string value,
    3: required RequestType request_type,
}

struct RedisResponse {
    1: optional string value,
    2: required ResponseType response_type,
}

service RedisService {
    RedisResponse RedisCommand (1: RedisRequest req),
}

