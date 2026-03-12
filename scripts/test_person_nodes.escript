#!/usr/bin/env escript
%%! -pa _build/default/lib/neo4j_ex/ebin -pa _build/default/lib/poolboy/ebin

main(_Args) ->
    io:format("Testing Neo4j Person nodes...~n"),
    io:format("~s~n", [lists:duplicate(50, $=)]),

    case neo4j_ex:start_link("bolt://localhost:7687", [{auth, {"neo4j", "password@12"}}]) of
        {ok, Driver} ->
            io:format("~n1. Creating Person nodes:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),

            CreateQuery = <<"CREATE (a:Person {name: $name, age: $age}) RETURN a">>,
            Persons = [
                {<<"Alice">>, 30},
                {<<"Bob">>, 25},
                {<<"Carol">>, 35}
            ],

            lists:foreach(fun({Name, Age}) ->
                case neo4j_ex:run(Driver, CreateQuery, #{<<"name">> => Name, <<"age">> => Age}) of
                    {ok, _} ->
                        io:format("  Created: ~s (age ~p)~n", [Name, Age]);
                    {error, Reason} ->
                        io:format("  Failed to create ~s: ~p~n", [Name, Reason])
                end
            end, Persons),

            io:format("~n2. Querying Person nodes:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),

            case neo4j_ex:run(Driver, <<"MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY p.age">>) of
                {ok, #{records := Records}} ->
                    io:format("  Found ~p persons:~n", [length(Records)]),
                    lists:foreach(fun(R) ->
                        Name = neo4j_record:get(R, <<"name">>),
                        Age = neo4j_record:get(R, <<"age">>),
                        io:format("    ~s (age ~p)~n", [Name, Age])
                    end, Records);
                {error, Reason} ->
                    io:format("  Query failed: ~p~n", [Reason])
            end,

            io:format("~n3. Cleanup - deleting test nodes:~n"),
            io:format("~s~n", [lists:duplicate(50, $-)]),

            case neo4j_ex:run(Driver, <<"MATCH (p:Person) WHERE p.name IN ['Alice', 'Bob', 'Carol'] DELETE p">>) of
                {ok, _} ->
                    io:format("  Cleanup complete.~n");
                {error, Reason2} ->
                    io:format("  Cleanup failed: ~p~n", [Reason2])
            end,

            neo4j_ex:close(Driver),
            io:format("~n~s~n", [lists:duplicate(50, $=)]),
            io:format("Test complete!~n");

        {error, Reason} ->
            io:format("Connection failed: ~p~n", [Reason])
    end.
