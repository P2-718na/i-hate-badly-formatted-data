import networkx as nx
import osmnx as ox

ox.settings.use_cache = True

# download street network data from OSM and construct a MultiDiGraph model
#G = ox.graph.graph_from_point((44.4050965, 8.902363), dist=8000, network_type="drive", simplify=True, retain_all=False)
G = ox.graph.graph_from_place( "Genova", network_type="drive", simplify=True, retain_all=False)
print("Download")
# impute edge (driving) speeds and calculate edge travel times
G = ox.routing.add_edge_speeds(G)
G = ox.routing.add_edge_travel_times(G)

# you can convert MultiDiGraph to/from GeoPandas GeoDataFrames
gdf_nodes, gdf_edges = ox.convert.graph_to_gdfs(G)
G = ox.convert.graph_from_gdfs(gdf_nodes, gdf_edges, graph_attrs=G.graph)

# convert MultiDiGraph to DiGraph to use nx.betweenness_centrality function
# choose between parallel edges by minimizing travel_time attribute value
D = ox.convert.to_digraph(G, weight="travel_time")

# calculate node betweenness centrality, weighted by travel time
bc = nx.betweenness_centrality(D, weight="travel_time", normalized=True)
nx.set_node_attributes(G, values=bc, name="bc")

# plot the graph, coloring nodes by betweenness centrality
nc = ox.plot.get_node_colors_by_attr(G, "bc", cmap="plasma")

fig, ax = ox.plot.plot_graph(
    G, bgcolor="k", node_size=50, node_color=nc, edge_linewidth=2, edge_color="#333333", save=True, filepath="graph.png"
)
##

# save graph as a geopackage or graphml file
# ox.io.save_graph_geopackage(G, filepath="./graph.gpkg")
# ox.io.save_graphml(G, filepath="./graph.graphml")