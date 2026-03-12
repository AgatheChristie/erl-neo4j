-module(neo4j_type_node).

-include("neo4j.hrl").

-export([
    new/3, new/4,
    get_property/2, has_label/2,
    labels/1, properties/1, id/1, element_id/1
]).

new(Id, Labels, Properties) ->
    #neo4j_node{id = Id, labels = Labels, properties = Properties}.

new(Id, Labels, Properties, ElementId) ->
    #neo4j_node{id = Id, labels = Labels, properties = Properties, element_id = ElementId}.

get_property(#neo4j_node{properties = Props}, Key) when is_atom(Key) ->
    maps:get(atom_to_binary(Key, utf8), Props, undefined);
get_property(#neo4j_node{properties = Props}, Key) when is_binary(Key) ->
    maps:get(Key, Props, undefined).

has_label(#neo4j_node{labels = Labels}, Label) ->
    lists:member(Label, Labels).

labels(#neo4j_node{labels = Labels}) -> Labels.

properties(#neo4j_node{properties = Props}) -> Props.

id(#neo4j_node{id = Id}) -> Id.

element_id(#neo4j_node{element_id = EId}) -> EId.
