-module(neo4j_type_path).

-include("neo4j.hrl").

-export([
    new/3,
    nodes/1, relationships/1, length/1,
    start_node/1, end_node/1
]).

new(Nodes, Relationships, Indices) ->
    #neo4j_path{nodes = Nodes, relationships = Relationships, indices = Indices}.

nodes(#neo4j_path{nodes = Nodes}) -> Nodes.

relationships(#neo4j_path{relationships = Rels}) -> Rels.

length(#neo4j_path{relationships = Rels}) -> erlang:length(Rels).

start_node(#neo4j_path{nodes = []}) -> undefined;
start_node(#neo4j_path{nodes = [First | _]}) -> First.

end_node(#neo4j_path{nodes = []}) -> undefined;
end_node(#neo4j_path{nodes = Nodes}) -> lists:last(Nodes).
