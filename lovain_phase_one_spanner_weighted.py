#
# WEIGHTED
#
# First time required only
#!gcloud auth application-default login
#!pip install --quiet google-cloud-spanner
import google.cloud.spanner as spanner

# Generate a new community mapping, all nodes begin in different communities
def generate_newcommunity(transaction):
    row_ct = transaction.execute_update(
        f"""INSERT INTO {communities_table} (fecha,cuits,community) SELECT CURRENT_DATE(),cuits,SUBSTR(cuits,3) FROM GRAPH_TABLE(CuitsTransfers MATCH (c:cuits) RETURN c.cuits)"""
    )
    print("{} record(s) inserted.".format(row_ct))
# Get the comunity from a specific node
def get_node_community(transaction, node_id):
    """Fetches the community of a node"""
    result = transaction.execute_sql(
        f"""
        SELECT community FROM {communities_table}
        WHERE cuits = @node_id and fecha = CURRENT_DATE()
        """,
        params={"node_id": node_id},
    )

    return list(result)[0][0]
# Update node community
def update_community(transaction,node_id,new_community):
    """Updates the community assignment for a node in Spanner."""
    transaction.execute_update(
        f"""
        UPDATE {communities_table}
        SET community = @new_community
        WHERE cuits = @node_id and fecha = CURRENT_DATE()
        """,
        params={"node_id": node_id, "new_community": new_community},
    )
def get_total_edges(transaction):
    """Calculates the total weight of all edges in the graph (m)."""
    sum_of_degrees = 0
    all_nodes = get_all_communities(transaction)
    for node in all_nodes:
      degree = sum(weight for _, weight in get_neighbors(transaction, node))
      sum_of_degrees += degree
    return sum_of_degrees / 2
def get_all_communities(transaction):
    """Fetches all communities from Spanner."""
    communities = {}
    results = transaction.execute_sql(
        f"SELECT cuits, community FROM {communities_table} where fecha = CURRENT_DATE()"
    )
    for row in results:
        communities[row[0]] = row[1]
    return communities
def get_neighbors(transaction, node_id):
    """Fetches the neighbors and edge weights for a given node from Spanner."""
    neighbors = []
    results = transaction.execute_sql(
        f"""GRAPH CuitsTransfers
          MATCH (c:cuits {{cuits:@node_id}})-[flow:TRANSFER]-(:cuits)
          RETURN
          CASE  WHEN flow.crid = @node_id THEN flow.cuits
              ELSE flow.crid
          END
            AS node_id,flow.importe AS weight
        """,
        params={"node_id": node_id},
    )
    for row in results:
        neighbors.append(row)
    return neighbors
def get_community_leaders(transaction):
    """Identifies community leaders based on weighted degree centrality (strength) after Phase 1."""
    leaders = {}  # {community_id: leader_node_id}

    with database.snapshot() as snapshot:
        communities = get_all_communities(snapshot)

    community_nodes = {}  # {community_id: [node_id1, node_id2, ...]}
    for node_id, community in communities.items():
        if community not in community_nodes:
            community_nodes[community] = []
        community_nodes[community].append(node_id)

    for community, nodes in community_nodes.items():
        max_strength = -1  # Initialize with a very small value
        leader = None
        for node in nodes:
            with database.snapshot() as snapshot:
                strength = sum(weight for _, weight in get_neighbors(snapshot, node))

            if strength > max_strength:
                max_strength = strength
                leader = node

        leaders[community] = leader

    return leaders
def calculate_modularity_change_spanner(transaction, node_id, current_community, new_community, total_edges):
    """
    Calculates the change in modularity if a node were to move to a new community.
    This modularity function is for weighted graphs.
    """
    # 1. k_i (degree of node i)
    k_i = sum(weight for _, weight in get_neighbors(transaction, node_id))

    # 2. k_i,in (sum of weights of edges from node i to nodes in the new_community)
    k_i_in = sum(
        weight
        for neighbor, weight in get_neighbors(transaction, node_id)
        if get_node_community(transaction, neighbor) == new_community
    )
    # 3. m (total weight of all edges in the graph)
    m = total_edges
    # 4. Σ_in (sum of weights of edges inside new_community)
    sigma_in = sum(
        weight
        for n1 in get_all_communities(transaction)
        if get_node_community(transaction, n1) == new_community
        for n2, weight in get_neighbors(transaction, n1)
        if get_node_community(transaction, n2) == new_community
    )
    # 5. Σ_tot (sum of degrees of nodes in new_community)
    sigma_tot = sum(
        sum(weight for _, weight in get_neighbors(transaction, n))
        for n in get_all_communities(transaction)
        if get_node_community(transaction, n) == new_community
    )
    # 5. Σ_in_old (sum of weights of edges inside current_community)
    sigma_in_old = sum(
        weight
        for n1 in get_all_communities(transaction)
        if get_node_community(transaction, n1) == current_community
        for n2, weight in get_neighbors(transaction, n1)
        if get_node_community(transaction, n2) == current_community
    )
    # 6. Σ_tot_old (sum of degrees of nodes in current_community)
    sigma_tot_old = sum(
        sum(weight for _, weight in get_neighbors(transaction, n))
        for n in get_all_communities(transaction)
        if get_node_community(transaction, n) == current_community
    )

    delta_q = (
        ((sigma_in + k_i_in) / (2 * m))
        - (((sigma_tot + k_i) / (2 * m)) ** 2)
        - ((sigma_in_old) / (2 * m))
        + (((sigma_tot_old) / (2 * m)) ** 2)
        - ((k_i / (2 * m)) ** 2)
    )

    return delta_q

#Main function
def louvain_phase_one_spanner(instance_id, database_id):
    improved = True
    while improved:
        improved = False
        with database.snapshot(multi_use=True) as snapshot:
          communities = get_all_communities(snapshot)
          total_edges = get_total_edges(snapshot)
          nodes = list(communities.keys())
        print(f"Nodes:  {nodes}")

        for node in nodes:
            print(f"Node:  {node}")
            with database.snapshot(multi_use=True) as snapshot:
              current_community = get_node_community(snapshot, node)
              neighbors = get_neighbors(snapshot, node)

            best_delta_q = 0
            best_community = current_community
            neighbor_communities = set()
            with database.snapshot(multi_use=True) as snapshot:
                  best_delta_q = calculate_modularity_change_spanner(snapshot, node, current_community, current_community, total_edges)

            for neighbor, _ in neighbors:
                with database.snapshot(multi_use=True) as snapshot:
                 neighbor_communities.add(get_node_community(snapshot, neighbor))
            for neighbor_community in neighbor_communities:
                if neighbor_community != current_community:
                  with database.snapshot(multi_use=True) as snapshot:
                    delta_q = calculate_modularity_change_spanner(snapshot, node, current_community, neighbor_community, total_edges)

                  if delta_q > best_delta_q:
                    #print(f"  Moved node {node} from community {current_community} to {neighbor_community} the new deltaq was {delta_q} and the old one is {best_delta_q}")  # Debug print
                    best_delta_q = delta_q
                    best_community = neighbor_community


            if best_community != current_community:
                database.run_in_transaction(update_community,node_id=node, new_community=best_community)
                improved = True

    # Calculating Communities leaders:
    with database.snapshot(multi_use=True) as snapshot:
        leaders = get_community_leaders(snapshot)
        for community, leader in leaders.items():
            print(f"Community: {community}, Leader: {leader}")
    return improved

# Example Usage (replace with your actual instance, database, and table names):
instance_id = "jblab"
database_id = "finance-graph-db"
communities_table = "cuits_communities"  # Replace with your communities table name
spanner_client = spanner.Client()
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

improved = louvain_phase_one_spanner(instance_id, database_id)
print("Improved:", improved)
