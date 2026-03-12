-module(neo4j_handshake_tests).
-include_lib("eunit/include/eunit.hrl").
-include("neo4j.hrl").

%% ===================================================================
%% Handshake data
%% ===================================================================

build_handshake_data_test() ->
    Data = neo4j_handshake:build_handshake_data(),
    ?assertEqual(20, byte_size(Data)),
    <<Magic:4/binary, _Rest/binary>> = Data,
    ?assertEqual(?BOLT_MAGIC, Magic).

encode_version_test() ->
    ?assertEqual(<<4, 0, 0, 5>>, neo4j_handshake:encode_version({5, 4})),
    ?assertEqual(<<3, 0, 0, 5>>, neo4j_handshake:encode_version({5, 3})).

%% ===================================================================
%% Version parsing
%% ===================================================================

parse_version_standard_test() ->
    ?assertEqual({ok, {5, 4}}, neo4j_handshake:parse_version(<<4, 0, 0, 5>>)).

parse_version_alt1_test() ->
    ?assertEqual({ok, {5, 3}}, neo4j_handshake:parse_version(<<0, 0, 3, 5>>)).

parse_version_alt2_test() ->
    ?assertEqual({ok, {4, 4}}, neo4j_handshake:parse_version(<<4, 4, 0, 0>>)).

parse_version_alt3_test() ->
    ?assertEqual({ok, {5, 1}}, neo4j_handshake:parse_version(<<0, 5, 0, 1>>)).

parse_version_invalid_test() ->
    ?assertMatch({error, invalid_version_format}, neo4j_handshake:parse_version(<<1, 2, 3, 4>>)).

%% ===================================================================
%% Version support
%% ===================================================================

supported_version_test() ->
    ?assertEqual(true, neo4j_handshake:supported_version({5, 4})),
    ?assertEqual(true, neo4j_handshake:supported_version({4, 3})),
    ?assertEqual(false, neo4j_handshake:supported_version({3, 0})).

supported_versions_test() ->
    Versions = neo4j_handshake:supported_versions(),
    ?assertEqual(4, length(Versions)),
    ?assert(lists:member({5, 4}, Versions)).
