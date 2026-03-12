-module(neo4j_summary).

-include("neo4j.hrl").

-export([
    new/1,
    query_type/1, counters/1, plan/1, profile/1,
    notifications/1, result_available_after/1, result_consumed_after/1,
    server/1, database/1,
    contains_updates/1, contains_system_updates/1,
    get_counter/2, to_map/1
]).

new(Metadata) when is_map(Metadata) ->
    #neo4j_summary{
        query_type = maps:get(<<"type">>, Metadata, undefined),
        counters = maps:get(<<"stats">>, Metadata, undefined),
        plan = maps:get(<<"plan">>, Metadata, undefined),
        profile = maps:get(<<"profile">>, Metadata, undefined),
        notifications = maps:get(<<"notifications">>, Metadata, undefined),
        result_available_after = maps:get(<<"result_available_after">>, Metadata,
                                    maps:get(<<"t_first">>, Metadata, undefined)),
        result_consumed_after = maps:get(<<"result_consumed_after">>, Metadata,
                                    maps:get(<<"t_last">>, Metadata, undefined)),
        server = maps:get(<<"server">>, Metadata, undefined),
        database = maps:get(<<"db">>, Metadata, undefined)
    }.

query_type(#neo4j_summary{query_type = QT}) -> QT.
counters(#neo4j_summary{counters = C}) -> C.
plan(#neo4j_summary{plan = P}) -> P.
profile(#neo4j_summary{profile = P}) -> P.

notifications(#neo4j_summary{notifications = undefined}) -> [];
notifications(#neo4j_summary{notifications = N}) -> N.

result_available_after(#neo4j_summary{result_available_after = T}) -> T.
result_consumed_after(#neo4j_summary{result_consumed_after = T}) -> T.
server(#neo4j_summary{server = S}) -> S.
database(#neo4j_summary{database = D}) -> D.

contains_updates(#neo4j_summary{counters = undefined}) -> false;
contains_updates(#neo4j_summary{counters = Counters}) when is_map(Counters) ->
    UpdateKeys = [
        <<"nodes_created">>, <<"nodes_deleted">>,
        <<"relationships_created">>, <<"relationships_deleted">>,
        <<"properties_set">>, <<"labels_added">>, <<"labels_removed">>,
        <<"indexes_added">>, <<"indexes_removed">>,
        <<"constraints_added">>, <<"constraints_removed">>
    ],
    lists:any(
        fun(Key) ->
            case maps:get(Key, Counters, 0) of
                0 -> false;
                _ -> true
            end
        end,
        UpdateKeys
    ).

contains_system_updates(#neo4j_summary{counters = undefined}) -> false;
contains_system_updates(#neo4j_summary{counters = Counters}) when is_map(Counters) ->
    case maps:get(<<"system_updates">>, Counters, 0) of
        0 -> false;
        _ -> true
    end.

get_counter(#neo4j_summary{counters = undefined}, _Name) -> 0;
get_counter(#neo4j_summary{counters = Counters}, Name) when is_map(Counters) ->
    maps:get(Name, Counters, 0).

to_map(#neo4j_summary{} = S) ->
    #{
        query_type => S#neo4j_summary.query_type,
        counters => S#neo4j_summary.counters,
        plan => S#neo4j_summary.plan,
        profile => S#neo4j_summary.profile,
        notifications => S#neo4j_summary.notifications,
        result_available_after => S#neo4j_summary.result_available_after,
        result_consumed_after => S#neo4j_summary.result_consumed_after,
        server => S#neo4j_summary.server,
        database => S#neo4j_summary.database
    }.
