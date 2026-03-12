-module(neo4j_stream).

-export([
    run/2, run/3, run/4,
    next/1
]).

-record(stream_state, {
    driver :: pid(),
    query :: binary(),
    params :: map(),
    batch_size :: integer(),
    timeout :: integer(),
    skip :: integer(),
    continue :: boolean()
}).

%% ===================================================================
%% Public API
%% ===================================================================

run(Driver, Query) ->
    run(Driver, Query, #{}, []).

run(Driver, Query, Params) ->
    run(Driver, Query, Params, []).

run(Driver, Query, Params, Opts) ->
    case neo4j_registry:lookup(Driver) of
        {ok, ResolvedDriver} ->
            BatchSize = proplists:get_value(batch_size, Opts, 1000),
            Timeout = proplists:get_value(timeout, Opts, 30000),
            State = #stream_state{
                driver = ResolvedDriver,
                query = Query,
                params = Params,
                batch_size = BatchSize,
                timeout = Timeout,
                skip = 0,
                continue = true
            },
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

next(#stream_state{continue = false}) ->
    done;
next(#stream_state{driver = Driver, query = Query, params = Params,
                   batch_size = BatchSize, timeout = Timeout, skip = Skip} = State) ->
    case fetch_batch(Driver, Query, Params, Skip, BatchSize, Timeout) of
        {ok, Records, BatchCount} ->
            NewSkip = Skip + BatchCount,
            Continue = BatchCount > 0 andalso BatchCount =:= BatchSize,
            NewState = State#stream_state{skip = NewSkip, continue = Continue},
            {ok, Records, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

%% ===================================================================
%% Internal
%% ===================================================================

fetch_batch(Driver, Query, Params, Skip, Limit, Timeout) ->
    neo4j_driver:session(Driver, fun(Session) ->
        PaginatedQuery = add_pagination(Query, Skip, Limit),
        case neo4j_session:run(Session, PaginatedQuery, Params, [{timeout, Timeout}]) of
            {ok, #{records := Records}} ->
                {ok, Records, length(Records)};
            {error, Reason} ->
                {error, Reason}
        end
    end).

add_pagination(Query, Skip, Limit) when is_binary(Query) ->
    SkipBin = integer_to_binary(Skip),
    LimitBin = integer_to_binary(Limit),
    Trimmed = string:trim(Query),
    iolist_to_binary([Trimmed, <<" SKIP ">>, SkipBin, <<" LIMIT ">>, LimitBin]);
add_pagination(Query, Skip, Limit) when is_list(Query) ->
    add_pagination(list_to_binary(Query), Skip, Limit).
