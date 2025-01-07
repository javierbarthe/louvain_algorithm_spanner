#
# UNWEIGHTED
#
# First time required only
# !gcloud auth application-default login
# !pip install --quiet google-cloud-spanner
import google.cloud.spanner as spanner
from google.cloud.spanner_v1 import param_types
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

    for row in result: # Iterate directly over the StreamedResultSet
        return row[0]  # Return the community ID from the first row

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
        param_types={"node_id": param_types.STRING, "new_community": param_types.STRING}
    )
# get total edges count if unweighted or sum of weights in case weighted
def get_total_edges_unweighted(transaction):
    """Calculates the total number of edges from Spanner."""
    result = transaction.execute_sql(
        #reemplazar por gql
        f"""GRAPH CuitsTransfers
            MATCH (c:cuits)-[flow:TRANSFER]->(:cuits)
            RETURN COUNT(flow.id) as TotalEdges"""
    )
    return list(result)[0][0]


def get_all_communities(transaction):
    """Fetches all communities from Spanner."""
    communities = {}
    results = transaction.execute_sql(
        f"SELECT cuits, community FROM {communities_table} where fecha = CURRENT_DATE()"
    )
    for row in results:
        communities[row[0]] = row[1]
    return communities

def get_neighbors_unweighted(transaction, node_id):
    """Fetches the neighbors and edge weights for a given node from Spanner."""
    neighbors = []
    results = transaction.execute_sql(
        f"""GRAPH CuitsTransfers
          MATCH (c:cuits {{cuits:@node_id}})-[flow:TRANSFER]-(:cuits)
          RETURN
          CASE  WHEN flow.crid = @node_id THEN flow.cuits
              ELSE flow.crid
          END
            AS node_id
        """,
        params={"node_id": node_id},
    )
    for row in results:
        neighbors.append(row[0])

    return neighbors

def get_community_leaders_unweighted(transaction):
    """Identifies community leaders based on degree centrality after Phase 1."""
    leaders = {}  # {community_id: leader_node_id}

    with database.snapshot() as snapshot:
      communities = get_all_communities(snapshot)
    print(communities)
    community_nodes = {}  # {community_id: [node_id1, node_id2, ...]}
    for node_id, community in communities.items():
        if community not in community_nodes:
            community_nodes[community] = []
        community_nodes[community].append(node_id)
    print (community_nodes)
    for community, nodes in community_nodes.items():
        max_degree = -1
        leader = None
        for node in nodes:
            with database.snapshot() as snapshot:
                degree = len(get_neighbors_unweighted(snapshot, node))  # Assuming unweighted

            if degree > max_degree:
                max_degree = degree
                leader = node

        leaders[community] = leader

    return leaders

def calculate_modularity_change_spanner_unweighted(transaction, node_id, current_community, new_community, total_edges):
    """
    Calculates the change in modularity if a node were to move to a new community.

    This modularity function is for UNWEIGHTED graphs.
    """

    # 1. k_i (degree of node i)
    k_i = len(get_neighbors_unweighted(transaction, node_id))

    # 2. k_i,in (number of links between node i and the nodes in the new_community)
    k_i_in = sum(
        1  # Count 1 for each edge (unweighted)
        for neighbor in get_neighbors_unweighted(transaction, node_id)
        if get_node_community(transaction, neighbor) == new_community
    )

    # 3. m (total number of links in the graph)
    m = total_edges

    # 4. Σ_in (NOT USED in the simplified formula for unweighted graphs)
    sigma_in = 0

    # 5. Σ_tot (sum of degrees of nodes in new_community)
    sigma_tot = sum(
        len(get_neighbors_unweighted(transaction, n))
        for n in get_all_communities(transaction)
        if get_node_community(transaction, n) == new_community
    )

    # 6. Σ_in_old (NOT USED in the simplified formula for unweighted graphs)
    sigma_in_old = 0

    # 7. Σ_tot_old (sum of degrees of nodes in current_community)
    sigma_tot_old = sum(
        len(get_neighbors_unweighted(transaction, n))
        for n in get_all_communities(transaction)
        if get_node_community(transaction, n) == current_community
    )

    # Calculate delta_q using the simplified formula for unweighted graphs:
    # ΔQ = [k_i,in / m] - [Σ_tot * k_i / 2m²]

    delta_q = (k_i_in / m) - (sigma_tot * k_i / (2 * (m**2)))

    return delta_q

def actualiza_comunidades_finales_nodos (transaction,snapshot):
    # 1. Read from 'communities' table (Read-Only Transaction)
      results = snapshot.execute_sql(
          f"SELECT cuits,community FROM {communities_table}" # Assuming community_id is the relevant column
      )
      # 2. Process and Update within the Same Loop
      for row in results:
          new_community = str(row[1])
          node_id = str(row[0])
           # 3. Update 'another_table' (Read-Write Transaction)
          transaction.execute_update(
              f"""
              UPDATE {nodes_table} c
              SET community = @new_community
              WHERE c.cuits = @node_id
              """,
              params={"node_id": node_id, "new_community": new_community},
              param_types={"node_id": param_types.STRING, "new_community": param_types.STRING}
          )

def actualiza_lideres_nodos (transaction, node_id):
      """Updates the community assignment for a node in Spanner."""
      transaction.execute_update(
        f"""
        UPDATE {nodes_table} c
        SET lider = True
        WHERE c.cuits = @node_id
        """,
        params={"node_id": node_id},
        param_types={"node_id": param_types.STRING}
      )

#Main function
def louvain_phase_one_spanner(instance_id, database_id,process_same_community_neighbors=False):
    
    with database.snapshot(multi_use=True) as snapshot:
      rerun=get_all_communities(snapshot)
      if len(rerun) == 0:
        print("Generando comunidades para procesamiento.....")
        database.run_in_transaction(generate_newcommunity)
      total_edges = get_total_edges_unweighted(snapshot)
    improved = True
    while improved:
        improved = False
        with database.snapshot() as snapshot:
          communities = get_all_communities(snapshot)
          nodes = list(communities.keys())
        # print(f"Nodes:  {nodes}")

        for node in nodes:
            print(f"Node:  {node}")
            with database.snapshot(multi_use=True) as snapshot:
              current_community = get_node_community(snapshot, node)
              # print(f"Current Community:  {current_community}")
              neighbors = get_neighbors_unweighted(snapshot, node)
            # print(f"Neighbors:  {neighbors}")
            #print(f"Total Edges:  {total_edges}")
            best_delta_q = 0
            best_community = current_community
            neighbor_communities = set()
            with database.snapshot(multi_use=True) as snapshot:
                  best_delta_q = calculate_modularity_change_spanner_unweighted(snapshot, node, current_community, current_community, total_edges)
            for neighbor in neighbors:
                with database.snapshot() as snapshot:
                 #print(f"Neighbor:  {neighbor}")
                 neighbor_communities.add(get_node_community(snapshot, neighbor))
            # print(f"Neighbor Communities:  {neighbor_communities}")
            for neighbor_community in neighbor_communities:
                if neighbor_community != current_community:
                  with database.snapshot(multi_use=True) as snapshot:
                    delta_q = calculate_modularity_change_spanner_unweighted(snapshot, node, current_community, neighbor_community, total_edges)

                  if delta_q > best_delta_q:
                    # print(f"  Moved node {node} from community {current_community} to {neighbor_community} the new deltaq was {delta_q} and the old one is {best_delta_q}")  # Debug print
                    best_delta_q = delta_q
                    best_community = neighbor_community

            if best_community != current_community:
                database.run_in_transaction(update_community,node_id=node, new_community=best_community)
                improved = True
                #print(f"Ingreso a cambio de community")
    
    #  Calculating and updating Communities with leaders:
    with database.snapshot(multi_use=True) as snapshot:
      database.run_in_transaction(actualiza_comunidades_finales_nodos,snapshot)
      leaders = get_community_leaders_unweighted(snapshot)
      for community, leader in leaders.items():
        print(f"Community: {community}, Leader: {leader}")
        database.run_in_transaction(actualiza_lideres_nodos,node_id=leader)
    return improved

# Example Usage (replace with your actual instance, database, and table names):
instance_id = "jblab"
database_id = "finance-graph-db"
communities_table = "cuits_communities"  # Replace with your communities table name
nodes_table = "cuits"  # Replace with your nodes table name
spanner_client = spanner.Client()
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

improved = louvain_phase_one_spanner(instance_id, database_id)
print("Improved:", improved)
