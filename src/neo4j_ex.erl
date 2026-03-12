-module(neo4j_ex).

-export([
    start_link/1, start_link/2,
    run/1, run/2, run/3, run/4,
    stream/1, stream/2, stream/3, stream/4,
    session/1, session/2,
    transaction/1, transaction/2,
    close/1, get_config/1, version/0,
    start_pool/1, stop_pool/0, stop_pool/1,
    pool_run/1, pool_run/2, pool_run/3,
    pool_transaction/1, pool_transaction/2,
    pool_status/0, pool_status/1
]).

%% ===================================================================
%% Driver Management
%% ===================================================================

start_link(Uri) ->
    start_link(Uri, []).

start_link(Uri, Opts) ->
    neo4j_driver:start_link(Uri, Opts).

%% ===================================================================
%% Query Execution
%% ===================================================================

run(Query) when is_binary(Query) ->
    run(default, Query, #{}, []).

run(Query, Params) when is_binary(Query), is_map(Params) ->
    run(default, Query, Params, []);
run(Driver, Query) ->
    run(Driver, Query, #{}, []).

run(Query, Params, Opts) when is_binary(Query), is_map(Params), is_list(Opts) ->
    run(default, Query, Params, Opts);
run(Driver, Query, Params) ->
    run(Driver, Query, Params, []).

run(Driver, Query, Params, Opts) ->
    case resolve_driver(Driver) of
        {ok, ResolvedDriver} ->
            neo4j_driver:run(ResolvedDriver, Query, Params, Opts);
        {error, _} = Err ->
            Err
    end.

%% ===================================================================
%% Streaming
%% ===================================================================

stream(Query) when is_binary(Query) ->
    stream(default, Query, #{}, []).

stream(Query, Params) when is_binary(Query), is_map(Params) ->
    stream(default, Query, Params, []);
stream(Driver, Query) ->
    stream(Driver, Query, #{}, []).

stream(Query, Params, Opts) when is_binary(Query), is_map(Params), is_list(Opts) ->
    stream(default, Query, Params, Opts);
stream(Driver, Query, Params) ->
    stream(Driver, Query, Params, []).

stream(Driver, Query, Params, Opts) ->
    case resolve_driver(Driver) of
        {ok, ResolvedDriver} ->
            neo4j_stream:run(ResolvedDriver, Query, Params, Opts);
        {error, _} = Err ->
            Err
    end.

%% ===================================================================
%% Session
%% ===================================================================

session(Fun) when is_function(Fun, 1) ->
    session(default, Fun).

session(Driver, Fun) when is_function(Fun, 1) ->
    case resolve_driver(Driver) of
        {ok, ResolvedDriver} ->
            neo4j_driver:session(ResolvedDriver, Fun);
        {error, _} = Err ->
            Err
    end.

%% ===================================================================
%% Transaction
%% ===================================================================

transaction(Fun) when is_function(Fun, 1) ->
    transaction(default, Fun).

transaction(Driver, Fun) when is_function(Fun, 1) ->
    case resolve_driver(Driver) of
        {ok, ResolvedDriver} ->
            neo4j_driver:transaction(ResolvedDriver, Fun);
        {error, _} = Err ->
            Err
    end.

%% ===================================================================
%% Utility
%% ===================================================================

close(Driver) ->
    neo4j_driver:close(Driver).

get_config(Driver) ->
    neo4j_driver:get_config(Driver).

version() ->
    case application:get_key(neo4j_ex, vsn) of
        {ok, Vsn} -> list_to_binary(Vsn);
        undefined -> <<"0.1.9">>
    end.

%% ===================================================================
%% Connection Pool API
%% ===================================================================

start_pool(Opts) ->
    neo4j_pool:start_pool(Opts).

stop_pool() ->
    neo4j_pool:stop_pool().

stop_pool(PoolName) ->
    neo4j_pool:stop_pool(PoolName).

pool_run(Query) -> pool_run(Query, #{}, []).
pool_run(Query, Params) -> pool_run(Query, Params, []).
pool_run(Query, Params, Opts) ->
    neo4j_pool:run(Query, Params, Opts).

pool_transaction(Fun) -> pool_transaction(Fun, []).
pool_transaction(Fun, Opts) ->
    neo4j_pool:transaction(Fun, Opts).

pool_status() -> neo4j_pool:status().
pool_status(PoolName) -> neo4j_pool:status(PoolName).

%% ===================================================================
%% Internal
%% ===================================================================

resolve_driver(DriverRef) ->
    neo4j_registry:lookup(DriverRef).
