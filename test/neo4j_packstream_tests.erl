-module(neo4j_packstream_tests).
-include_lib("eunit/include/eunit.hrl").
-include("neo4j.hrl").

%% ===================================================================
%% Null / Bool
%% ===================================================================

encode_null_test() ->
    ?assertEqual(<<16#C0>>, neo4j_packstream:encode(undefined)).

encode_false_test() ->
    ?assertEqual(<<16#C2>>, neo4j_packstream:encode(false)).

encode_true_test() ->
    ?assertEqual(<<16#C3>>, neo4j_packstream:encode(true)).

decode_null_test() ->
    ?assertEqual({ok, undefined, <<>>}, neo4j_packstream:decode(<<16#C0>>)).

decode_false_test() ->
    ?assertEqual({ok, false, <<>>}, neo4j_packstream:decode(<<16#C2>>)).

decode_true_test() ->
    ?assertEqual({ok, true, <<>>}, neo4j_packstream:decode(<<16#C3>>)).

%% ===================================================================
%% Integers
%% ===================================================================

encode_tiny_int_test() ->
    ?assertEqual(<<0>>, neo4j_packstream:encode(0)),
    ?assertEqual(<<1>>, neo4j_packstream:encode(1)),
    ?assertEqual(<<127>>, neo4j_packstream:encode(127)),
    ?assertEqual(<<16#F0>>, neo4j_packstream:encode(-16)).

encode_int8_test() ->
    ?assertEqual(<<16#C8, 128:8/signed>>, neo4j_packstream:encode(-128)).

encode_int16_test() ->
    ?assertEqual(<<16#C9, 1000:16/signed>>, neo4j_packstream:encode(1000)).

encode_int32_test() ->
    ?assertEqual(<<16#CA, 100000:32/signed>>, neo4j_packstream:encode(100000)).

encode_int64_test() ->
    ?assertEqual(<<16#CB, 3000000000:64/signed>>, neo4j_packstream:encode(3000000000)).

decode_tiny_int_test() ->
    ?assertEqual({ok, 0, <<>>}, neo4j_packstream:decode(<<0>>)),
    ?assertEqual({ok, 42, <<>>}, neo4j_packstream:decode(<<42>>)).

roundtrip_int_test() ->
    Values = [0, 1, -1, 127, -16, -128, 1000, -1000, 100000, -100000, 3000000000],
    lists:foreach(fun(V) ->
        Encoded = neo4j_packstream:encode(V),
        {ok, Decoded, <<>>} = neo4j_packstream:decode(Encoded),
        ?assertEqual(V, Decoded)
    end, Values).

%% ===================================================================
%% Float
%% ===================================================================

encode_float_test() ->
    Encoded = neo4j_packstream:encode(3.14),
    ?assertEqual(<<16#C1, 3.14:64/float>>, Encoded).

roundtrip_float_test() ->
    V = 3.14159,
    {ok, Decoded, <<>>} = neo4j_packstream:decode(neo4j_packstream:encode(V)),
    ?assert(abs(V - Decoded) < 0.00001).

%% ===================================================================
%% String
%% ===================================================================

encode_tiny_string_test() ->
    ?assertEqual(<<16#80>>, neo4j_packstream:encode(<<>>)),
    ?assertEqual(<<16#85, "hello">>, neo4j_packstream:encode(<<"hello">>)).

roundtrip_string_test() ->
    Values = [<<>>, <<"hello">>, <<"world">>, <<"a longer string for testing">>],
    lists:foreach(fun(V) ->
        Encoded = neo4j_packstream:encode(V),
        {ok, Decoded, <<>>} = neo4j_packstream:decode(Encoded),
        ?assertEqual(V, Decoded)
    end, Values).

%% ===================================================================
%% List
%% ===================================================================

encode_empty_list_test() ->
    ?assertEqual(<<16#90>>, neo4j_packstream:encode([])).

roundtrip_list_test() ->
    Values = [[], [1, 2, 3], [<<"a">>, <<"b">>], [1, <<"two">>, true, undefined]],
    lists:foreach(fun(V) ->
        Encoded = neo4j_packstream:encode(V),
        {ok, Decoded, <<>>} = neo4j_packstream:decode(Encoded),
        ?assertEqual(V, Decoded)
    end, Values).

%% ===================================================================
%% Map
%% ===================================================================

encode_empty_map_test() ->
    ?assertEqual(<<16#A0>>, neo4j_packstream:encode(#{})).

roundtrip_map_test() ->
    V = #{<<"name">> => <<"Alice">>, <<"age">> => 30},
    Encoded = neo4j_packstream:encode(V),
    {ok, Decoded, <<>>} = neo4j_packstream:decode(Encoded),
    ?assertEqual(V, Decoded).

%% ===================================================================
%% Struct
%% ===================================================================

encode_struct_test() ->
    V = {struct, 16#01, [#{<<"user_agent">> => <<"test">>}]},
    Encoded = neo4j_packstream:encode(V),
    {ok, Decoded, <<>>} = neo4j_packstream:decode(Encoded),
    ?assertMatch({struct, 16#01, [#{<<"user_agent">> := <<"test">>}]}, Decoded).

%% ===================================================================
%% decode_all
%% ===================================================================

decode_all_test() ->
    D1 = neo4j_packstream:encode(1),
    D2 = neo4j_packstream:encode(<<"hello">>),
    Combined = <<D1/binary, D2/binary>>,
    ?assertEqual({ok, [1, <<"hello">>]}, neo4j_packstream:decode_all(Combined)).

decode_all_empty_test() ->
    ?assertEqual({ok, []}, neo4j_packstream:decode_all(<<>>)).
