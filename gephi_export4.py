
import sqlite3
import csv
from collections import defaultdict
from itertools import combinations

DB_FILE = 'pernet.db'
EDGES_FILE = 'gephi_edges_per_sessie.csv'
NODES_FILE = 'gephi_nodes.csv'

def haal_sessie_artiesten():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT session_artists.session_id, artists.name
        FROM session_artists
        JOIN artists ON session_artists.artist_id = artists.artist_id
        ORDER BY session_artists.session_id
    """)
    rows = c.fetchall()
    conn.close()

    sessies = defaultdict(list)
    for sessie_id, naam in rows:
        sessies[sessie_id].append(naam)
    return sessies

def schrijf_edges_per_sessie(sessies):
    """Schrijf edges per sessie naar CSV (Source, Target, Session_ID)."""
    with open(EDGES_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Source', 'Target', 'Session_ID'])
        for sessie_id, artiesten in sessies.items():
            if len(artiesten) >= 2:
                # Unieke paren (gesorteerd om duplicaten te voorkomen binnen de sessie)
                for a, b in combinations(sorted(artiesten), 2):
                    writer.writerow([a, b, sessie_id])
    print(f"{sum(len(v)*(len(v)-1)//2 for v in sessies.values())} edges weggeschreven naar {EDGES_FILE}")

def schrijf_nodes(sessies):
    """Schrijf nodes naar CSV (Id, Label, Sessions, Degree)."""
    # Tel aantal sessies per artiest
    sessie_counts = defaultdict(int)
    for artiesten in sessies.values():
        for a in artiesten:
            sessie_counts[a] += 1

    # Tel graad (aantal unieke buren) – niet gebaseerd op gewicht, maar op alle edges.
    degree = defaultdict(int)
    for artiesten in sessies.values():
        if len(artiesten) >= 2:
            for a, b in combinations(artiesten, 2):
                degree[a] += 1
                degree[b] += 1

    with open(NODES_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Id', 'Label', 'Sessions', 'Degree'])
        for naam in sorted(sessie_counts.keys()):
            writer.writerow([naam, naam, sessie_counts[naam], degree[naam]])
    print(f"{len(sessie_counts)} nodes weggeschreven naar {NODES_FILE}")

def main():
    print("Data ophalen...")
    sessies = haal_sessie_artiesten()
    print(f"Aantal sessies: {len(sessies)}")
    schrijf_edges_per_sessie(sessies)
    schrijf_nodes(sessies)
    print("Klaar.")

if __name__ == "__main__":
    main()
