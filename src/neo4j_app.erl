-module(neo4j_app).
-behaviour(application).

-export([start/2, stop/1]).
-export([build_children/0, get_single_driver_config/0, build_driver_child_spec/2]).

start(_StartType, _StartArgs) ->
    Children = build_children(),
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    neo4j_sup:start_link(SupFlags, Children).

stop(_State) ->
    ok.

build_children() ->
    PoolSup = #{
        id => neo4j_pool_sup,
        start => {neo4j_pool_sup, start_link, []},
        type => supervisor
    },
    DriverChildren = case application:get_env(neo4j_ex, drivers) of
        {ok, Drivers} when is_list(Drivers) ->
            lists:filtermap(
                fun({Name, Config}) ->
                    case build_driver_child_spec(Name, Config) of
                        undefined -> false;
                        Spec -> {true, Spec}
                    end
                end,
                Drivers
            );
        undefined ->
            case get_single_driver_config() of
                undefined -> [];
                Config -> [build_driver_child_spec(default, Config)]
            end
    end,
    [PoolSup | DriverChildren].

get_single_driver_config() ->
    case application:get_env(neo4j_ex, uri) of
        {ok, Uri} ->
            Config0 = [{uri, Uri}],
            Config1 = maybe_add_env(Config0, auth),
            Config2 = maybe_add_env(Config1, connection_timeout),
            Config3 = maybe_add_env(Config2, query_timeout),
            maybe_add_env(Config3, user_agent);
        undefined ->
            undefined
    end.

build_driver_child_spec(Name, Config) ->
    case proplists:get_value(uri, Config) of
        undefined ->
            logger:warning("Neo4j driver ~p missing uri configuration, skipping", [Name]),
            undefined;
        Uri ->
            Opts = [{name, Name} | Config],
            #{
                id => Name,
                start => {neo4j_driver, start_link, [Uri, Opts]}
            }
    end.

%% Internal

maybe_add_env(Config, Key) ->
    case application:get_env(neo4j_ex, Key) of
        {ok, Val} -> [{Key, Val} | Config];
        undefined -> Config
    end.
