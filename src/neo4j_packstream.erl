-module(neo4j_packstream).

-include("neo4j.hrl").

-export([encode/1, decode/1, decode_all/1]).

%% ===================================================================
%% Encoding
%% ===================================================================

encode(undefined) -> <<?PACKSTREAM_NULL>>;
encode(null) -> <<?PACKSTREAM_NULL>>;
encode(false) -> <<?PACKSTREAM_FALSE>>;
encode(true) -> <<?PACKSTREAM_TRUE>>;

encode(V) when is_integer(V) -> encode_integer(V);
encode(V) when is_float(V) -> encode_float(V);
encode(V) when is_binary(V) -> encode_string(V);
encode(V) when is_list(V) -> encode_list(V);
encode(V) when is_atom(V) -> encode_string(atom_to_binary(V, utf8));

encode({struct, Sig, Fields}) -> encode_struct(Sig, Fields);

encode(#neo4j_point2d{} = P) ->
    Fields = neo4j_types:encode_point(P),
    encode_struct(?NEO4J_POINT2D_SIG, Fields);
encode(#neo4j_point3d{} = P) ->
    Fields = neo4j_types:encode_point(P),
    encode_struct(?NEO4J_POINT3D_SIG, Fields);
encode(#neo4j_date{} = D) ->
    Fields = neo4j_types:encode_date(D),
    encode_struct(?NEO4J_DATE_SIG, Fields);
encode(#neo4j_time{} = T) ->
    Fields = neo4j_types:encode_time(T),
    encode_struct(?NEO4J_TIME_SIG, Fields);
encode(#neo4j_local_time{} = T) ->
    Fields = neo4j_types:encode_local_time(T),
    encode_struct(?NEO4J_LOCAL_TIME_SIG, Fields);
encode(#neo4j_datetime{} = DT) ->
    Fields = neo4j_types:encode_datetime(DT),
    encode_struct(?NEO4J_DATETIME_SIG, Fields);
encode(#neo4j_local_datetime{} = DT) ->
    Fields = neo4j_types:encode_local_datetime(DT),
    encode_struct(?NEO4J_LOCAL_DATETIME_SIG, Fields);
encode(#neo4j_duration{} = D) ->
    Fields = neo4j_types:encode_duration(D),
    encode_struct(?NEO4J_DURATION_SIG, Fields);

encode(V) when is_map(V) -> encode_map(V);

encode(V) -> error({cannot_encode, V}).

%% Integer encoding
encode_integer(N) when N >= -16, N =< 127 ->
    <<N:8/signed>>;
encode_integer(N) when N >= -128, N =< 127 ->
    <<?PACKSTREAM_INT8, N:8/signed>>;
encode_integer(N) when N >= -32768, N =< 32767 ->
    <<?PACKSTREAM_INT16, N:16/signed>>;
encode_integer(N) when N >= -2147483648, N =< 2147483647 ->
    <<?PACKSTREAM_INT32, N:32/signed>>;
encode_integer(N) ->
    <<?PACKSTREAM_INT64, N:64/signed>>.

%% Float encoding
encode_float(F) ->
    <<?PACKSTREAM_FLOAT64, F:64/float>>.

%% String encoding
encode_string(S) when is_binary(S) ->
    Size = byte_size(S),
    if
        Size =< 15 ->
            Marker = ?PACKSTREAM_TINY_STRING bor Size,
            <<Marker, S/binary>>;
        Size =< 255 ->
            <<?PACKSTREAM_STRING8, Size:8, S/binary>>;
        Size =< 65535 ->
            <<?PACKSTREAM_STRING16, Size:16, S/binary>>;
        Size =< 4294967295 ->
            <<?PACKSTREAM_STRING32, Size:32, S/binary>>;
        true ->
            error({string_too_large, Size})
    end.

%% List encoding
encode_list(List) when is_list(List) ->
    Size = length(List),
    EncodedItems = iolist_to_binary([encode(Item) || Item <- List]),
    if
        Size =< 15 ->
            Marker = ?PACKSTREAM_TINY_LIST bor Size,
            <<Marker, EncodedItems/binary>>;
        Size =< 255 ->
            <<?PACKSTREAM_LIST8, Size:8, EncodedItems/binary>>;
        Size =< 65535 ->
            <<?PACKSTREAM_LIST16, Size:16, EncodedItems/binary>>;
        Size =< 4294967295 ->
            <<?PACKSTREAM_LIST32, Size:32, EncodedItems/binary>>;
        true ->
            error({list_too_large, Size})
    end.

%% Map encoding
encode_map(Map) when is_map(Map) ->
    Size = map_size(Map),
    EncodedPairs = iolist_to_binary(
        maps:fold(
            fun(K, V, Acc) ->
                KeyBin = if
                    is_binary(K) -> K;
                    is_atom(K) -> atom_to_binary(K, utf8);
                    true -> iolist_to_binary(io_lib:format("~p", [K]))
                end,
                [Acc, encode(KeyBin), encode(V)]
            end,
            [],
            Map
        )
    ),
    if
        Size =< 15 ->
            Marker = ?PACKSTREAM_TINY_MAP bor Size,
            <<Marker, EncodedPairs/binary>>;
        Size =< 255 ->
            <<?PACKSTREAM_MAP8, Size:8, EncodedPairs/binary>>;
        Size =< 65535 ->
            <<?PACKSTREAM_MAP16, Size:16, EncodedPairs/binary>>;
        Size =< 4294967295 ->
            <<?PACKSTREAM_MAP32, Size:32, EncodedPairs/binary>>;
        true ->
            error({map_too_large, Size})
    end.

%% Structure encoding
encode_struct(Signature, Fields) when is_integer(Signature), is_list(Fields) ->
    Size = length(Fields),
    EncodedFields = iolist_to_binary([encode(F) || F <- Fields]),
    if
        Size =< 15 ->
            Marker = ?PACKSTREAM_TINY_STRUCT bor Size,
            <<Marker, Signature:8, EncodedFields/binary>>;
        Size =< 255 ->
            <<?PACKSTREAM_STRUCT8, Size:8, Signature:8, EncodedFields/binary>>;
        Size =< 65535 ->
            <<?PACKSTREAM_STRUCT16, Size:16, Signature:8, EncodedFields/binary>>;
        true ->
            error({struct_too_large, Size})
    end.

%% ===================================================================
%% Decoding
%% ===================================================================

decode(<<?PACKSTREAM_NULL, Rest/binary>>) -> {ok, undefined, Rest};
decode(<<?PACKSTREAM_FALSE, Rest/binary>>) -> {ok, false, Rest};
decode(<<?PACKSTREAM_TRUE, Rest/binary>>) -> {ok, true, Rest};

%% Integers
decode(<<?PACKSTREAM_INT8, N:8/signed, Rest/binary>>) -> {ok, N, Rest};
decode(<<?PACKSTREAM_INT16, N:16/signed, Rest/binary>>) -> {ok, N, Rest};
decode(<<?PACKSTREAM_INT32, N:32/signed, Rest/binary>>) -> {ok, N, Rest};
decode(<<?PACKSTREAM_INT64, N:64/signed, Rest/binary>>) -> {ok, N, Rest};

%% Float
decode(<<?PACKSTREAM_FLOAT64, F:64/float, Rest/binary>>) -> {ok, F, Rest};

%% Strings
decode(<<Marker, Rest/binary>>) when (Marker band 16#F0) =:= ?PACKSTREAM_TINY_STRING ->
    Size = Marker band 16#0F,
    decode_string(Size, Rest);
decode(<<?PACKSTREAM_STRING8, Size:8, Rest/binary>>) -> decode_string(Size, Rest);
decode(<<?PACKSTREAM_STRING16, Size:16, Rest/binary>>) -> decode_string(Size, Rest);
decode(<<?PACKSTREAM_STRING32, Size:32, Rest/binary>>) -> decode_string(Size, Rest);

%% Lists
decode(<<Marker, Rest/binary>>) when (Marker band 16#F0) =:= ?PACKSTREAM_TINY_LIST ->
    Size = Marker band 16#0F,
    decode_list(Size, Rest);
decode(<<?PACKSTREAM_LIST8, Size:8, Rest/binary>>) -> decode_list(Size, Rest);
decode(<<?PACKSTREAM_LIST16, Size:16, Rest/binary>>) -> decode_list(Size, Rest);
decode(<<?PACKSTREAM_LIST32, Size:32, Rest/binary>>) -> decode_list(Size, Rest);

%% Maps
decode(<<Marker, Rest/binary>>) when (Marker band 16#F0) =:= ?PACKSTREAM_TINY_MAP ->
    Size = Marker band 16#0F,
    decode_map(Size, Rest);
decode(<<?PACKSTREAM_MAP8, Size:8, Rest/binary>>) -> decode_map(Size, Rest);
decode(<<?PACKSTREAM_MAP16, Size:16, Rest/binary>>) -> decode_map(Size, Rest);
decode(<<?PACKSTREAM_MAP32, Size:32, Rest/binary>>) -> decode_map(Size, Rest);

%% Structures
decode(<<Marker, Signature:8, Rest/binary>>) when (Marker band 16#F0) =:= ?PACKSTREAM_TINY_STRUCT ->
    Size = Marker band 16#0F,
    decode_struct(Signature, Size, Rest);
decode(<<?PACKSTREAM_STRUCT8, Size:8, Signature:8, Rest/binary>>) ->
    decode_struct(Signature, Size, Rest);
decode(<<?PACKSTREAM_STRUCT16, Size:16, Signature:8, Rest/binary>>) ->
    decode_struct(Signature, Size, Rest);

%% Tiny int (must come after all specific markers)
decode(<<N:8/signed, Rest/binary>>) when N >= -16, N =< 127 ->
    {ok, N, Rest};

decode(<<>>) -> {error, incomplete};
decode(_) -> {error, invalid_format}.

%% ===================================================================
%% Decode Helpers
%% ===================================================================

decode_string(Size, Data) ->
    case Data of
        <<String:Size/binary, Rest/binary>> ->
            {ok, String, Rest};
        _ ->
            {error, incomplete}
    end.

decode_list(0, Rest) -> {ok, [], Rest};
decode_list(Size, Data) ->
    decode_list_items(Size, Data, []).

decode_list_items(0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Rest};
decode_list_items(N, Data, Acc) ->
    case decode(Data) of
        {ok, Value, Rest} ->
            decode_list_items(N - 1, Rest, [Value | Acc]);
        Error ->
            Error
    end.

decode_map(0, Rest) -> {ok, #{}, Rest};
decode_map(Size, Data) ->
    decode_map_pairs(Size, Data, #{}).

decode_map_pairs(0, Rest, Acc) ->
    {ok, Acc, Rest};
decode_map_pairs(N, Data, Acc) ->
    case decode(Data) of
        {ok, Key, Rest1} ->
            case decode(Rest1) of
                {ok, Value, Rest2} ->
                    decode_map_pairs(N - 1, Rest2, Acc#{Key => Value});
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

decode_struct(Signature, Size, Data) ->
    case decode_list_items(Size, Data, []) of
        {ok, Fields, Rest} ->
            StructValue = convert_neo4j_struct(Signature, Fields),
            {ok, StructValue, Rest};
        Error ->
            Error
    end.

%% ===================================================================
%% Neo4j Struct Conversion
%% ===================================================================

convert_neo4j_struct(?NEO4J_NODE_SIG, [Id, Labels, Props]) ->
    neo4j_type_node:new(Id, Labels, Props);
convert_neo4j_struct(?NEO4J_NODE_SIG, [Id, Labels, Props, ElementId]) ->
    neo4j_type_node:new(Id, Labels, Props, ElementId);

convert_neo4j_struct(?NEO4J_RELATIONSHIP_SIG, [Id, StartId, EndId, Type, Props]) ->
    neo4j_type_rel:new(Id, StartId, EndId, Type, Props);
convert_neo4j_struct(?NEO4J_RELATIONSHIP_SIG, [Id, StartId, EndId, Type, Props, ElementId]) ->
    neo4j_type_rel:new(Id, StartId, EndId, Type, Props, ElementId);

convert_neo4j_struct(?NEO4J_PATH_SIG, [Nodes, Rels, Indices]) ->
    neo4j_type_path:new(Nodes, Rels, Indices);

convert_neo4j_struct(?NEO4J_POINT2D_SIG, Fields) ->
    neo4j_types:decode_point(Fields);
convert_neo4j_struct(?NEO4J_POINT3D_SIG, Fields) ->
    neo4j_types:decode_point(Fields);

convert_neo4j_struct(?NEO4J_DATE_SIG, Fields) ->
    neo4j_types:decode_date(Fields);
convert_neo4j_struct(?NEO4J_TIME_SIG, Fields) ->
    neo4j_types:decode_time(Fields);
convert_neo4j_struct(?NEO4J_LOCAL_TIME_SIG, Fields) ->
    neo4j_types:decode_local_time(Fields);

convert_neo4j_struct(?NEO4J_DATETIME_SIG, Fields) ->
    neo4j_types:decode_datetime(Fields);
convert_neo4j_struct(?NEO4J_DATETIME_LEGACY_SIG, Fields) ->
    neo4j_types:decode_datetime(Fields);
convert_neo4j_struct(?NEO4J_DATETIME_ZONE_ID_SIG, Fields) ->
    neo4j_types:decode_datetime(Fields);

convert_neo4j_struct(?NEO4J_LOCAL_DATETIME_SIG, Fields) ->
    neo4j_types:decode_local_datetime(Fields);
convert_neo4j_struct(?NEO4J_DURATION_SIG, Fields) ->
    neo4j_types:decode_duration(Fields);

convert_neo4j_struct(Signature, Fields) ->
    {struct, Signature, Fields}.

%% ===================================================================
%% Decode All
%% ===================================================================

decode_all(Data) ->
    decode_all(Data, []).

decode_all(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_all(Data, Acc) ->
    case decode(Data) of
        {ok, Value, Rest} ->
            decode_all(Rest, [Value | Acc]);
        {error, Reason} ->
            {error, Reason}
    end.
