import networkx as nx
import random
import datetime
import pandas as pd

def generate_transaction_graph(num_accounts, num_edges, model="ER"):
    """Generates a synthetic transaction graph."""

    if model == "ER":
        graph = nx.DiGraph(nx.erdos_renyi_graph(num_accounts, num_edges / (num_accounts * (num_accounts - 1)))) #probability is calculated to get the desired number of edges
    elif model == "BA":
        # BA model requires m (edges to attach from new node)
        m = int(num_edges / num_accounts) if num_accounts > 0 else 1 #avoid division by zero
        m = max(1, m) #m must be at least 1
        graph = nx.DiGraph(nx.barabasi_albert_graph(num_accounts, m))
    elif model == "WS":
        # WS model requires k (neighbors to connect to) and p (rewiring probability)
        k = int(num_edges / num_accounts) if num_accounts > 0 else 1
        k = max(1, k)
        p = 0.1  # Adjust rewiring probability as needed
        graph = nx.DiGraph(nx.watts_strogatz_graph(num_accounts, k, p))
    else:
        raise ValueError("Invalid model specified.")

    # Add transaction attributes
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2024, 1, 1)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days

    for u, v in graph.edges():
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + datetime.timedelta(days=random_number_of_days)
        random_time = datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        timestamp = datetime.datetime.combine(random_date, random_time)

        graph[u][v]["amount"] = round(random.uniform(10, 1000), 2)  # Random amount between 10 and 1000
        graph[u][v]["timestamp"] = timestamp
        graph[u][v]["transaction_type"] = random.choice(["transfer"])

    return graph

def graph_to_df(graph):
    """Converts a NetworkX graph with edge attributes to a Pandas DataFrame."""
    edges = []
    for u, v, data in graph.edges(data=True):
        edge_data = {"source": u, "target": v}
        edge_data.update(data)
        edges.append(edge_data)
    return pd.DataFrame(edges)

num_accounts = 10000
num_edges = 50000

ba_graph = generate_transaction_graph(num_accounts, num_edges, model="BA")

print("BA Graph: Nodes =", ba_graph.number_of_nodes(), "Edges =", ba_graph.number_of_edges())

data = graph_to_df(ba_graph)
data
