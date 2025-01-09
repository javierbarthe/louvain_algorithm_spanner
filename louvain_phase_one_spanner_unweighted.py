#
# UNWEIGHTED
#
# First time required only
# !gcloud auth application-default login
# !pip install --quiet google-cloud-spanner
# delete from cuits_communities_test where fecha = CURRENT_DATE();
# select * from cuits_communities_test;
# INSERT INTO cuits_communities_test (fecha,cuits,community) SELECT CURRENT_DATE(),c.cuits,c.cuits FROM cuits_test c;
import google.cloud.spanner as spanner
from google.cloud.spanner_v1 import param_types
import time
# Generate a new community mapping, all nodes begin in different communities
def generate_newcommunity(transaction):
    row_ct = transaction.execute_update(
        f"""INSERT INTO {communities_table} (fecha,cuits,community) SELECT CURRENT_DATE(),cuits,cuits FROM GRAPH_TABLE(CuitsTransfers MATCH (c:cuits) RETURN c.cuits)"""
    )
    print("{} record(s) inserted.".format(row_ct))
# Get the comunity from a specific node
def get_node_community2(transaction, node_id):
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
        f"""Select count(id) as TotalEdges from {edges_table}"""
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
        f"""select CASE  WHEN crid = @node_id THEN cuits
                   ELSE crid
                   END
                   AS node_id
            From {edges_table} where cuits = @node_id or crid = @node_id
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

def calculate_modularity_change_spanner_unweighted(transaction, node_id, current_community, new_community, total_edges,node_degrees,node_communities,community_members,neighbors_list):
    """
    Calculates the change in modularity if a node were to move to a new community.

    This modularity function is for UNWEIGHTED graphs.
    """
    # 1. k_i (degree of node i) - now a fast lookup
    # print(f"node_id: {node_id}")
    # print(f"node_degree: {node_degrees[node_id]}")

    k_i = node_degrees[node_id]
    # print(f"k_i: {k_i}")

    # 2. k_i,in (optimized using precomputed community memberships)
    k_i_in = len([
        neighbor
        for neighbor in neighbors_list
        if node_communities.get(neighbor) == new_community
    ])
    # print(f"k_i_in: {k_i_in}")

    # 3. m (total number of links in the graph)
    m = total_edges
    # print(f"m: {m}")

    # 4. Σ_in (NOT USED)
    sigma_in = 0

    # 5. Σ_tot (optimized using precomputed degrees and community memberships)
    sigma_tot = sum(
        node_degrees[n]
        for n in community_members.get(new_community, []) # or new_community_members if you pre-filter community_dic
    )
    # print(f"sigma_tot: {sigma_tot}")

    # 6. Σ_in_old (NOT USED)
    sigma_in_old = 0

    # 7. Σ_tot_old (optimized)
    sigma_tot_old = sum(
        node_degrees[n]
        for n in community_members.get(current_community, []) # or current_community_members if you pre-filter community_dic
    )
    # print(f"sigma_tot_old: {sigma_tot_old}")

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
def louvain_phase_one_spanner(instance_id, database_id):
    with database.snapshot(multi_use=True) as snapshot:
      rerun=get_all_communities(snapshot)
      if len(rerun) == 0:
        print("Generando comunidades para procesamiento.....")
        database.run_in_transaction(generate_newcommunity)
      # Precompute degrees and community memberships outside the main loop
      # print("Precompute degrees")
      # start_time = time.time()  # Record the start time of the iteration
      total_edges = get_total_edges_unweighted(snapshot)
      nodess= list(rerun.keys())
      node_degrees = {nodo: len(get_neighbors_unweighted(snapshot, nodo)) for nodo in nodess}
      # end_time = time.time()  # Record the end time of the iteration
      # elapsed_time = end_time - start_time  # Calculate elapsed time
      # print(f"Precompute degrees: Time elapsed: {elapsed_time:.4f} seconds")
      # print("Precompute community memberships")
      nodesss= list(rerun.keys())
      node_communities = {noditos: get_node_community2(snapshot, noditos) for noditos in nodesss}
      # end_time = time.time()  # Record the end time of the iteration
      # elapsed_time = end_time - start_time  # Calculate elapsed time
      # print(f"Precompute community memberships: Time elapsed: {elapsed_time:.4f} seconds")
      # print(f"Node Degrees:  {node_degrees}")
      # print(f"Node Communities:  {node_communities}")
      # print(node_degrees[node])
      # you can use either community_members or pre-filter community_dic as explained in optimization 2

    improved = True
    while improved:
        improved = False
        with database.snapshot(multi_use=True) as snapshot:
          communities = get_all_communities(snapshot)
          community_list = list(communities.keys())
          nodes = list(communities.keys())
          # print(f"Nodes:  {nodes}")
          # print(f"Community List:  {community_list}")
        for node in nodes:
            print(f"Node:  {node}")
            start_time = time.time()  # Record the start time of the iteration
            with database.snapshot(multi_use=True) as snapshot:
              current_community = communities[node]
              # print(f"Current Community:  {current_community}")
              neighbors = get_neighbors_unweighted(snapshot, node)
              # print(f"Neighbors:  {neighbors}")
              # print(f"Total Edges:  {total_edges}")
            # print("Precompute community members")
            community_members = {}
            for nod, community in node_communities.items():
                    community_members.setdefault(community, []).append(nod)
            # end_time = time.time()  # Record the end time of the iteration
            # elapsed_time = end_time - start_time  # Calculate elapsed time
            # print(f"Precompute community members: Time elapsed: {elapsed_time:.4f} seconds")
            # print(f"Community Members:  {community_members}")
            best_delta_q = 0
            best_community = current_community
            neighbor_communities = set()
            with database.snapshot(multi_use=True) as snapshot:
                  # print("entro")
                  # print(f"Node:  {node}")
                  best_delta_q = calculate_modularity_change_spanner_unweighted(snapshot, node, current_community, current_community, total_edges,node_degrees,node_communities,community_members,neighbors)
                  # print(f"Node:  {node}")
                  # print("salio")
            for neighbor in neighbors:
                with database.snapshot() as snapshot:
                  # print(f"Neighbor:  {neighbor}")
                  # neighbor_communities.add(community_list[community_list.index(neighbor)])
                  neighbor_communities.add(communities[neighbor])
                  # print(f"Neighbor Community:  {communities[neighbor]}")
            # print(f"Neighbor Communities:  {neighbor_communities}")
            for neighbor_community in neighbor_communities:
                if neighbor_community != current_community:
                  with database.snapshot(multi_use=True) as snapshot:
                    delta_q = calculate_modularity_change_spanner_unweighted(snapshot, node, current_community, neighbor_community, total_edges,node_degrees,node_communities,community_members,neighbors)
                    # print(f"Neighbor Community:  {neighbor_community}")
                    # print(f"Delta Q:  {delta_q}")
                    # print(f"Best Delta Q:  {best_delta_q}")
                  if delta_q > best_delta_q:
                    print(f"  Moved node {node} from community {best_community} to {neighbor_community} the new deltaq was {delta_q} and the old one is {best_delta_q}")  # Debug print
                    best_delta_q = delta_q
                    best_community = neighbor_community
                  # end_time = time.time()  # Record the end time of the iteration
                  # elapsed_time = end_time - start_time  # Calculate elapsed time
                  # print(f"Vuelta {node}, para comunidad {neighbor_community} Time elapsed: {elapsed_time:.4f} seconds")
            if best_community != current_community:
                database.run_in_transaction(update_community,node_id=node, new_community=best_community)
                improved = True
                node_communities[node] = best_community

            end_time = time.time()  # Record the end time of the iteration
            elapsed_time = end_time - start_time  # Calculate elapsed time
            print(f"Iteration {node}: Time elapsed: {elapsed_time:.4f} seconds")
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
communities_table = "cuits_communities_test"  # Replace with your communities table name
nodes_table = "cuits_test"  # Replace with your nodes table name
edges_table = "transfers_test"  # Replace with your edges table name
spanner_client = spanner.Client()
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

improved = louvain_phase_one_spanner(instance_id, database_id)
print("Improved:", improved)
