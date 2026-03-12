-module(neo4j_types_tests).
-include_lib("eunit/include/eunit.hrl").
-include("neo4j.hrl").

%% ===================================================================
%% Point
%% ===================================================================

point_2d_test() ->
    P = neo4j_types:point_2d(40.7128, -74.006),
    ?assertEqual(4326, P#neo4j_point2d.srid),
    ?assert(is_float(P#neo4j_point2d.x)).

point_2d_custom_srid_test() ->
    P = neo4j_types:point_2d(100.0, 200.0, 7203),
    ?assertEqual(7203, P#neo4j_point2d.srid).

point_3d_test() ->
    P = neo4j_types:point_3d(40.7128, -74.006, 10.5),
    ?assertEqual(4979, P#neo4j_point3d.srid).

encode_decode_point2d_test() ->
    P = neo4j_types:point_2d(1.0, 2.0),
    Encoded = neo4j_types:encode_point(P),
    Decoded = neo4j_types:decode_point(Encoded),
    ?assertEqual(P, Decoded).

encode_decode_point3d_test() ->
    P = neo4j_types:point_3d(1.0, 2.0, 3.0),
    Encoded = neo4j_types:encode_point(P),
    Decoded = neo4j_types:decode_point(Encoded),
    ?assertEqual(P, Decoded).

%% ===================================================================
%% Date
%% ===================================================================

encode_decode_date_test() ->
    D = #neo4j_date{year = 2024, month = 1, day = 15},
    Encoded = neo4j_types:encode_date(D),
    Decoded = neo4j_types:decode_date(Encoded),
    ?assertEqual(D, Decoded).

epoch_date_test() ->
    D = neo4j_types:decode_date([0]),
    ?assertEqual(#neo4j_date{year = 1970, month = 1, day = 1}, D).

%% ===================================================================
%% Time
%% ===================================================================

encode_decode_time_test() ->
    T = #neo4j_time{hour = 10, minute = 30, second = 45, nanosecond = 0, timezone_offset_seconds = -18000},
    Encoded = neo4j_types:encode_time(T),
    Decoded = neo4j_types:decode_time(Encoded),
    ?assertEqual(T, Decoded).

%% ===================================================================
%% LocalTime
%% ===================================================================

encode_decode_local_time_test() ->
    T = #neo4j_local_time{hour = 10, minute = 30, second = 45, nanosecond = 123},
    Encoded = neo4j_types:encode_local_time(T),
    Decoded = neo4j_types:decode_local_time(Encoded),
    ?assertEqual(T, Decoded).

%% ===================================================================
%% DateTime
%% ===================================================================

decode_datetime_with_tz_id_test() ->
    DT = neo4j_types:decode_datetime([0, 0, <<"UTC">>]),
    ?assertEqual(1970, DT#neo4j_datetime.year),
    ?assertEqual(<<"UTC">>, DT#neo4j_datetime.timezone_id).

decode_datetime_with_offset_test() ->
    DT = neo4j_types:decode_datetime([0, 0, 3600]),
    ?assertMatch(<<"+01:00">>, DT#neo4j_datetime.timezone_id).

%% ===================================================================
%% LocalDateTime
%% ===================================================================

encode_decode_local_datetime_test() ->
    DT = #neo4j_local_datetime{year = 2024, month = 6, day = 15,
                               hour = 10, minute = 30, second = 0, nanosecond = 500},
    Encoded = neo4j_types:encode_local_datetime(DT),
    Decoded = neo4j_types:decode_local_datetime(Encoded),
    ?assertEqual(DT#neo4j_local_datetime.year, Decoded#neo4j_local_datetime.year),
    ?assertEqual(DT#neo4j_local_datetime.nanosecond, Decoded#neo4j_local_datetime.nanosecond).

%% ===================================================================
%% Duration
%% ===================================================================

encode_decode_duration_test() ->
    D = #neo4j_duration{months = 12, days = 30, seconds = 3600, nanoseconds = 123},
    Encoded = neo4j_types:encode_duration(D),
    Decoded = neo4j_types:decode_duration(Encoded),
    ?assertEqual(D, Decoded).

%% ===================================================================
%% Node / Relationship / Path
%% ===================================================================

node_test() ->
    N = neo4j_type_node:new(1, [<<"Person">>], #{<<"name">> => <<"Alice">>}),
    ?assertEqual(1, neo4j_type_node:id(N)),
    ?assertEqual([<<"Person">>], neo4j_type_node:labels(N)),
    ?assertEqual(<<"Alice">>, neo4j_type_node:get_property(N, <<"name">>)),
    ?assertEqual(true, neo4j_type_node:has_label(N, <<"Person">>)),
    ?assertEqual(false, neo4j_type_node:has_label(N, <<"Animal">>)).

relationship_test() ->
    R = neo4j_type_rel:new(10, 1, 2, <<"KNOWS">>, #{<<"since">> => 2020}),
    ?assertEqual(10, neo4j_type_rel:id(R)),
    ?assertEqual(1, neo4j_type_rel:start_id(R)),
    ?assertEqual(2, neo4j_type_rel:end_id(R)),
    ?assertEqual(<<"KNOWS">>, neo4j_type_rel:type(R)),
    ?assertEqual(2020, neo4j_type_rel:get_property(R, <<"since">>)).

path_test() ->
    N1 = neo4j_type_node:new(1, [], #{}),
    N2 = neo4j_type_node:new(2, [], #{}),
    R1 = neo4j_type_rel:new(10, 1, 2, <<"KNOWS">>, #{}),
    P = neo4j_type_path:new([N1, N2], [R1], [0]),
    ?assertEqual(1, neo4j_type_path:length(P)),
    ?assertEqual(N1, neo4j_type_path:start_node(P)),
    ?assertEqual(N2, neo4j_type_path:end_node(P)).

%% ===================================================================
%% Record / Summary
%% ===================================================================

record_get_by_index_test() ->
    R = neo4j_record:new([1, <<"Alice">>, 30], [<<"id">>, <<"name">>, <<"age">>]),
    ?assertEqual(1, neo4j_record:get(R, 0)),
    ?assertEqual(<<"Alice">>, neo4j_record:get(R, 1)),
    ?assertEqual(30, neo4j_record:get(R, 2)).

record_get_by_name_test() ->
    R = neo4j_record:new([1, <<"Alice">>, 30], [<<"id">>, <<"name">>, <<"age">>]),
    ?assertEqual(1, neo4j_record:get(R, <<"id">>)),
    ?assertEqual(<<"Alice">>, neo4j_record:get(R, <<"name">>)),
    ?assertEqual(30, neo4j_record:get(R, <<"age">>)).

record_to_map_test() ->
    R = neo4j_record:new([1, <<"Alice">>], [<<"id">>, <<"name">>]),
    ?assertEqual(#{<<"id">> => 1, <<"name">> => <<"Alice">>}, neo4j_record:to_map(R)).

record_size_test() ->
    R = neo4j_record:new([1, 2, 3]),
    ?assertEqual(3, neo4j_record:size(R)).

summary_new_test() ->
    Meta = #{<<"type">> => <<"r">>, <<"db">> => <<"neo4j">>},
    S = neo4j_summary:new(Meta),
    ?assertEqual(<<"r">>, neo4j_summary:query_type(S)),
    ?assertEqual(<<"neo4j">>, neo4j_summary:database(S)).

summary_contains_updates_test() ->
    S1 = neo4j_summary:new(#{<<"stats">> => #{<<"nodes_created">> => 1}}),
    S2 = neo4j_summary:new(#{}),
    ?assertEqual(true, neo4j_summary:contains_updates(S1)),
    ?assertEqual(false, neo4j_summary:contains_updates(S2)).

%% ===================================================================
%% Advanced type check
%% ===================================================================

advanced_type_test() ->
    ?assertEqual(true, neo4j_types:advanced_type(#neo4j_point2d{})),
    ?assertEqual(true, neo4j_types:advanced_type(#neo4j_date{})),
    ?assertEqual(true, neo4j_types:advanced_type(#neo4j_duration{})),
    ?assertEqual(false, neo4j_types:advanced_type(42)),
    ?assertEqual(false, neo4j_types:advanced_type(<<"hello">>)).
