-module(neo4j_pool).

-export([
    start_pool/1, stop_pool/0, stop_pool/1,
    checkout/0, checkout/1, checkout/2,
    checkin/1, checkin/2,
    run/1, run/2, run/3,
    transaction/1, transaction/2,
    status/0, status/1
]).

-define(DEFAULT_POOL_SIZE, 10).
-define(DEFAULT_MAX_OVERFLOW, 5).
-define(DEFAULT_POOL_NAME, ?MODULE).

%% ===================================================================
%% Public API
%% ===================================================================

start_pool(Opts) ->
    case proplists:get_value(uri, Opts) of
        undefined ->
            {error, {missing_required_option, uri}};
        Uri ->
            ConnConfig = parse_uri_and_opts(Uri, Opts),
            PoolSize = proplists:get_value(pool_size, Opts, ?DEFAULT_POOL_SIZE),
            MaxOverflow = proplists:get_value(max_overflow, Opts, ?DEFAULT_MAX_OVERFLOW),
            PoolName = proplists:get_value(name, Opts, ?DEFAULT_POOL_NAME),

            PoolboyConfig = [
                {name, {local, PoolName}},
                {worker_module, neo4j_pool_worker},
                {size, PoolSize},
                {max_overflow, MaxOverflow},
                {strategy, fifo}
            ],
            WorkerArgs = maps:to_list(ConnConfig),
            case poolboy:start_link(PoolboyConfig, WorkerArgs) of
                {ok, _Pid} -> {ok, PoolName};
                {error, {already_started, _Pid}} -> {ok, PoolName};
                Error -> Error
            end
    end.

stop_pool() -> stop_pool(?DEFAULT_POOL_NAME).
stop_pool(PoolName) -> poolboy:stop(PoolName).

checkout() -> checkout(?DEFAULT_POOL_NAME).
checkout(PoolName) -> checkout(PoolName, 5000).
checkout(PoolName, Timeout) -> poolboy:checkout(PoolName, true, Timeout).

checkin(Worker) -> checkin(?DEFAULT_POOL_NAME, Worker).
checkin(PoolName, Worker) -> poolboy:checkin(PoolName, Worker).

run(Query) -> run(Query, #{}).
run(Query, Params) -> run(Query, Params, []).
run(Query, Params, Opts) ->
    PoolName = proplists:get_value(pool_name, Opts, ?DEFAULT_POOL_NAME),
    Timeout = proplists:get_value(timeout, Opts, 30000),
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            neo4j_pool_worker:run(Worker, Query, Params, Opts)
        end,
        Timeout
    ).

transaction(Fun) -> transaction(Fun, []).
transaction(Fun, Opts) when is_function(Fun, 0) ->
    PoolName = proplists:get_value(pool_name, Opts, ?DEFAULT_POOL_NAME),
    Timeout = proplists:get_value(timeout, Opts, 30000),
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            neo4j_pool_worker:transaction(Worker, Fun, Opts)
        end,
        Timeout
    ).

status() -> status(?DEFAULT_POOL_NAME).
status(PoolName) ->
    case poolboy:status(PoolName) of
        {Status, Size, Overflow, Workers} ->
            #{status => Status, size => Size, overflow => Overflow, workers => Workers};
        Other ->
            Other
    end.

%% ===================================================================
%% Internal
%% ===================================================================

parse_uri_and_opts(Uri, Opts) ->
    UriConfig = parse_uri(Uri),
    Auth = normalize_auth(proplists:get_value(auth, Opts)),
    Config = #{
        host => maps:get(host, UriConfig, "localhost"),
        port => maps:get(port, UriConfig, 7687),
        auth => Auth,
        user_agent => proplists:get_value(user_agent, Opts, "neo4j_ex/0.1.0"),
        connection_timeout => proplists:get_value(connection_timeout, Opts, 15000),
        query_timeout => proplists:get_value(query_timeout, Opts, 30000)
    },
    Config.

parse_uri("bolt://" ++ Rest) ->
    case string:split(Rest, ":", trailing) of
        [Host, Port] -> #{host => Host, port => list_to_integer(Port)};
        [Host] -> #{host => Host}
    end;
parse_uri(<<"bolt://", Rest/binary>>) ->
    parse_uri("bolt://" ++ binary_to_list(Rest));
parse_uri(_) ->
    #{}.

normalize_auth(undefined) -> #{};
normalize_auth({Username, Password}) ->
    #{
        <<"scheme">> => <<"basic">>,
        <<"principal">> => to_bin(Username),
        <<"credentials">> => to_bin(Password)
    };
normalize_auth(Auth) when is_map(Auth) -> Auth.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V).
