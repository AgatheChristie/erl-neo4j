-module(neo4j_messages_tests).
-include_lib("eunit/include/eunit.hrl").
-include("neo4j.hrl").

%% ===================================================================
%% Message construction
%% ===================================================================

hello_test() ->
    Msg = neo4j_messages:hello(<<"neo4j_ex/0.1.0">>),
    ?assertMatch({struct, ?BOLT_HELLO, [#{<<"user_agent">> := <<"neo4j_ex/0.1.0">>}]}, Msg).

hello_with_auth_test() ->
    Auth = #{<<"scheme">> => <<"basic">>, <<"principal">> => <<"neo4j">>},
    Msg = neo4j_messages:hello(<<"agent">>, Auth),
    {struct, ?BOLT_HELLO, [Extra]} = Msg,
    ?assertEqual(<<"basic">>, maps:get(<<"scheme">>, Extra)),
    ?assertEqual(<<"agent">>, maps:get(<<"user_agent">>, Extra)).

goodbye_test() ->
    ?assertEqual({struct, ?BOLT_GOODBYE, []}, neo4j_messages:goodbye()).

reset_test() ->
    ?assertEqual({struct, ?BOLT_RESET, []}, neo4j_messages:reset()).

run_test() ->
    Msg = neo4j_messages:run(<<"RETURN 1">>),
    ?assertMatch({struct, ?BOLT_RUN, [<<"RETURN 1">>, #{}, #{}]}, Msg).

run_with_params_test() ->
    Msg = neo4j_messages:run(<<"RETURN $x">>, #{<<"x">> => 42}),
    ?assertMatch({struct, ?BOLT_RUN, [<<"RETURN $x">>, #{<<"x">> := 42}, #{}]}, Msg).

pull_test() ->
    Msg = neo4j_messages:pull(#{<<"n">> => -1}),
    ?assertMatch({struct, ?BOLT_PULL, [#{<<"n">> := -1}]}, Msg).

begin_tx_test() ->
    ?assertMatch({struct, ?BOLT_BEGIN, [#{}]}, neo4j_messages:begin_tx()).

commit_test() ->
    ?assertEqual({struct, ?BOLT_COMMIT, []}, neo4j_messages:commit()).

rollback_test() ->
    ?assertEqual({struct, ?BOLT_ROLLBACK, []}, neo4j_messages:rollback()).

logon_test() ->
    Msg = neo4j_messages:logon(<<"basic">>),
    {struct, ?BOLT_LOGON, [AuthMap]} = Msg,
    ?assertEqual(<<"basic">>, maps:get(<<"scheme">>, AuthMap)).

logoff_test() ->
    ?assertEqual({struct, ?BOLT_LOGOFF, []}, neo4j_messages:logoff()).

%% ===================================================================
%% Response parsing
%% ===================================================================

parse_success_test() ->
    ?assertEqual({success, #{<<"fields">> => [<<"n">>]}},
                 neo4j_messages:parse_response({struct, ?BOLT_SUCCESS, [#{<<"fields">> => [<<"n">>]}]})).

parse_failure_test() ->
    ?assertMatch({failure, #{<<"message">> := <<"error">>}},
                 neo4j_messages:parse_response({struct, ?BOLT_FAILURE, [#{<<"message">> => <<"error">>}]})).

parse_record_test() ->
    ?assertEqual({record, [1, <<"Alice">>]},
                 neo4j_messages:parse_response({struct, ?BOLT_RECORD, [[1, <<"Alice">>]]})).

parse_ignored_test() ->
    ?assertEqual({ignored, #{}},
                 neo4j_messages:parse_response({struct, ?BOLT_IGNORED, []})).

%% ===================================================================
%% Encode / Decode roundtrip
%% ===================================================================

encode_decode_message_test() ->
    Msg = neo4j_messages:run(<<"RETURN 1">>, #{}, #{}),
    Encoded = neo4j_messages:encode_message(Msg),
    {ok, Decoded, <<>>} = neo4j_messages:decode_message(Encoded),
    ?assertMatch({struct, ?BOLT_RUN, [<<"RETURN 1">>, #{}, #{}]}, Decoded).

encode_decode_hello_test() ->
    Msg = neo4j_messages:hello(<<"test_agent">>),
    Encoded = neo4j_messages:encode_message(Msg),
    {ok, Decoded, <<>>} = neo4j_messages:decode_message(Encoded),
    {struct, ?BOLT_HELLO, [Extra]} = Decoded,
    ?assertEqual(<<"test_agent">>, maps:get(<<"user_agent">>, Extra)).

decode_messages_test() ->
    Msg1 = neo4j_messages:encode_message({struct, ?BOLT_RECORD, [[1]]}),
    Msg2 = neo4j_messages:encode_message({struct, ?BOLT_SUCCESS, [#{}]}),
    Combined = <<Msg1/binary, Msg2/binary>>,
    {ok, Messages, <<>>} = neo4j_messages:decode_messages(Combined),
    ?assertEqual(2, length(Messages)).

%% ===================================================================
%% Utility
%% ===================================================================

signature_name_test() ->
    ?assertEqual("HELLO", neo4j_messages:signature_name(?BOLT_HELLO)),
    ?assertEqual("SUCCESS", neo4j_messages:signature_name(?BOLT_SUCCESS)),
    ?assertEqual("RECORD", neo4j_messages:signature_name(?BOLT_RECORD)).

summary_message_test() ->
    ?assertEqual(true, neo4j_messages:summary_message({struct, ?BOLT_SUCCESS, [#{}]})),
    ?assertEqual(true, neo4j_messages:summary_message({struct, ?BOLT_FAILURE, [#{}]})),
    ?assertEqual(false, neo4j_messages:summary_message({struct, ?BOLT_RECORD, [[1]]})).
