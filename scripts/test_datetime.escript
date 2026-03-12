#!/usr/bin/env escript
%%! -pa _build/default/lib/neo4j_ex/ebin -pa _build/default/lib/poolboy/ebin

main(_Args) ->
    io:format("Testing Neo4j datetime support...~n"),
    io:format("~s~n", [lists:duplicate(50, $=)]),

    case neo4j_ex:start_link("bolt://localhost:7687", [{auth, {"neo4j", "password@12"}}]) of
        {ok, Driver} ->
            io:format("~n1. Testing datetime() function:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),
            test_query(Driver, <<"RETURN datetime()">>),

            io:format("~n2. Testing time() function:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),
            test_query(Driver, <<"RETURN time()">>),

            io:format("~n3. Testing date() function:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),
            test_query(Driver, <<"RETURN date()">>),

            io:format("~n4. Testing localdatetime() function:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),
            test_query(Driver, <<"RETURN localdatetime()">>),

            io:format("~n5. Testing datetime with explicit timezone:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),
            test_query(Driver, <<"RETURN datetime({timezone: 'America/New_York'})">>),

            neo4j_ex:close(Driver),
            io:format("~n~s~n", [lists:duplicate(50, $=)]),
            io:format("Test complete!~n");

        {error, Reason} ->
            io:format("Connection failed: ~p~n", [Reason])
    end.

test_query(Driver, Query) ->
    case neo4j_ex:run(Driver, Query) of
        {ok, #{records := Records}} ->
            io:format("  Query successful!~n"),
            lists:foreach(fun(R) ->
                Values = neo4j_record:values(R),
                io:format("  Value: ~p~n", [Values])
            end, Records);
        {error, Reason} ->
            io:format("  Query failed: ~p~n", [Reason])
    end.
