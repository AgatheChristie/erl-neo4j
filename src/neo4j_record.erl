-module(neo4j_record).

-include("neo4j.hrl").

-export([
    new/1, new/2,
    get/2, values/1, fields/1,
    to_map/1, to_map/2, to_proplist/1, to_proplist/2,
    size/1, is_empty/1,
    foreach/2, map/2, foldl/3
]).

new(Values) ->
    #neo4j_record{values = Values, fields = undefined}.

new(Values, Fields) ->
    #neo4j_record{values = Values, fields = Fields}.

get(#neo4j_record{values = Values}, Key) when is_integer(Key) ->
    try lists:nth(Key + 1, Values)
    catch _:_ -> undefined
    end;
get(#neo4j_record{values = Values, fields = Fields}, Key) when is_binary(Key) ->
    case Fields of
        undefined -> undefined;
        FieldList ->
            case find_index(Key, FieldList, 0) of
                not_found -> undefined;
                Index -> lists:nth(Index + 1, Values)
            end
    end.

values(#neo4j_record{values = Values}) -> Values.

fields(#neo4j_record{fields = Fields}) -> Fields.

to_map(Record) -> to_map(Record, undefined).

to_map(#neo4j_record{values = Values, fields = Fields}, FieldNames) ->
    FList = case FieldNames of
        undefined -> Fields;
        _ -> FieldNames
    end,
    case FList of
        undefined -> #{};
        Names when is_list(Names) ->
            maps:from_list(lists:zip(Names, Values))
    end.

to_proplist(Record) -> to_proplist(Record, undefined).

to_proplist(#neo4j_record{values = Values, fields = Fields}, FieldNames) ->
    FList = case FieldNames of
        undefined -> Fields;
        _ -> FieldNames
    end,
    case FList of
        undefined -> [];
        Names when is_list(Names) ->
            AtomNames = [binary_to_atom(N, utf8) || N <- Names],
            lists:zip(AtomNames, Values)
    end.

size(#neo4j_record{values = Values}) -> erlang:length(Values).

is_empty(#neo4j_record{values = []}) -> true;
is_empty(#neo4j_record{}) -> false.

foreach(Fun, #neo4j_record{values = Values}) ->
    lists:foreach(Fun, Values).

map(Fun, #neo4j_record{values = Values}) ->
    lists:map(Fun, Values).

foldl(Fun, Acc0, #neo4j_record{values = Values}) ->
    lists:foldl(Fun, Acc0, Values).

%% Internal

find_index(_Key, [], _Idx) -> not_found;
find_index(Key, [Key | _], Idx) -> Idx;
find_index(Key, [_ | Rest], Idx) -> find_index(Key, Rest, Idx + 1).
