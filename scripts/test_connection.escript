#!/usr/bin/env escript
%%! -pa _build/default/lib/neo4j_ex/ebin -pa _build/default/lib/poolboy/ebin

main(_Args) ->
    io:format("Testing Neo4j connection...~n"),
    io:format("~s~n", [lists:duplicate(50, $=)]),

    Host = os:getenv("NEO4J_HOST", "localhost"),
    Port = list_to_integer(os:getenv("NEO4J_PORT", "7687")),
    User = os:getenv("NEO4J_USER", "neo4j"),
    Pass = os:getenv("NEO4J_PASS", "password@12"),

    Uri = "bolt://" ++ Host ++ ":" ++ integer_to_list(Port),
    io:format("Connecting to: ~s~n", [Uri]),

    case neo4j_ex:start_link(Uri, [{auth, {User, Pass}}]) of
        {ok, Driver} ->
            io:format("~nConnection successful!~n"),

            io:format("~n1. Testing simple query:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),

            case neo4j_ex:run(Driver, <<"RETURN 1 AS num">>) of
                {ok, #{records := Records}} ->
                    io:format("  Query returned ~p record(s)~n", [length(Records)]),
                    lists:foreach(fun(R) ->
                        io:format("  Record values: ~p~n", [neo4j_record:values(R)])
                    end, Records);
                {error, Reason} ->
                    io:format("  Query failed: ~p~n", [Reason])
            end,

            io:format("~n2. Testing parameterized query:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),

            case neo4j_ex:run(Driver, <<"RETURN $x + $y AS sum">>, #{<<"x">> => 10, <<"y">> => 20}) of
                {ok, #{records := Records2}} ->
                    io:format("  Query returned ~p record(s)~n", [length(Records2)]),
                    lists:foreach(fun(R) ->
                        io:format("  Record values: ~p~n", [neo4j_record:values(R)])
                    end, Records2);
                {error, Reason2} ->
                    io:format("  Query failed: ~p~n", [Reason2])
            end,

            neo4j_ex:close(Driver),
            io:format("~n~s~n", [lists:duplicate(50, $=)]),
            io:format("Test complete!~n");

        {error, Reason} ->
            io:format("Connection failed: ~p~n", [Reason])
    end.
