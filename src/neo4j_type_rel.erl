-module(neo4j_type_rel).

-include("neo4j.hrl").

-export([
    new/5, new/6,
    get_property/2, properties/1,
    id/1, type/1, start_id/1, end_id/1, element_id/1
]).

new(Id, StartId, EndId, Type, Properties) ->
    #neo4j_relationship{
        id = Id, start_id = StartId, end_id = EndId,
        type = Type, properties = Properties
    }.

new(Id, StartId, EndId, Type, Properties, ElementId) ->
    #neo4j_relationship{
        id = Id, start_id = StartId, end_id = EndId,
        type = Type, properties = Properties, element_id = ElementId
    }.

get_property(#neo4j_relationship{properties = Props}, Key) when is_atom(Key) ->
    maps:get(atom_to_binary(Key, utf8), Props, undefined);
get_property(#neo4j_relationship{properties = Props}, Key) when is_binary(Key) ->
    maps:get(Key, Props, undefined).

properties(#neo4j_relationship{properties = Props}) -> Props.

id(#neo4j_relationship{id = Id}) -> Id.

type(#neo4j_relationship{type = Type}) -> Type.

start_id(#neo4j_relationship{start_id = SId}) -> SId.

end_id(#neo4j_relationship{end_id = EId}) -> EId.

element_id(#neo4j_relationship{element_id = EId}) -> EId.
