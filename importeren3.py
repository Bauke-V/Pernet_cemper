#!/usr/bin/env python3


import xml.etree.ElementTree as ET
import sqlite3
import re
from collections import defaultdict

# Configuratie
DB_FILE = 'pernet.db'
XML_FILES = {
    'sessions': '20260318_Pernet_subset_sessies.xml',
    'countries': '20260318_pernet_landen.xml',
    'labels': '20260318_pernet_labels.xml',
    'compositions': '20260318_pernet_composities.xml',
    'releases': '20260318_pernet_releases.xml',
    'artists': '20260318_pernet_artiesten.xml'
}

def parse_xml(filename):
    try:
        tree = ET.parse(filename)
        return tree.getroot()
    except FileNotFoundError:
        print(f"Waarschuwing: bestand {filename} niet gevonden – wordt overgeslagen.")
        return None
    except ET.ParseError as e:
        print(f"Fout bij parsen van {filename}: {e}")
        return None

#databank aanmaken
conn = sqlite3.connect(DB_FILE)
conn.execute("PRAGMA foreign_keys = ON")
c = conn.cursor()

# Tabellen aanmaken 
c.executescript("""
DROP TABLE IF EXISTS session_artist_instruments;
DROP TABLE IF EXISTS instruments;
DROP TABLE IF EXISTS song_releases;
DROP TABLE IF EXISTS session_artists;
DROP TABLE IF EXISTS songs;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS artists;
DROP TABLE IF EXISTS releases;
DROP TABLE IF EXISTS compositions;
DROP TABLE IF EXISTS labels;
DROP TABLE IF EXISTS countries;

CREATE TABLE artists (
    artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    summary TEXT,
    pernet TEXT,
    pernet_note TEXT, 
    band TEXT,
    ymd TEXT,        
    year INTEGER,
    month INTEGER,
    day INTEGER,
    locations_extra TEXT,             
    country TEXT,
    city TEXT,
    venue TEXT,
    notes TEXT
);

CREATE TABLE session_artists (
    session_artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    artist_id INTEGER NOT NULL,
    instruments TEXT,          -- komma‑gescheiden lijst (uit instr-attribuut)
    info TEXT,
    alias TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id) ON DELETE CASCADE,
    UNIQUE (session_id, artist_id)
);

CREATE TABLE instruments (
    instrument_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE session_artist_instruments (
    session_artist_id INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    FOREIGN KEY (session_artist_id) REFERENCES session_artists(session_artist_id) ON DELETE CASCADE,
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    PRIMARY KEY (session_artist_id, instrument_id)
);

CREATE TABLE songs (
    song_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    title TEXT,
    compo TEXT,
    mx TEXT,
    take TEXT,
    credits TEXT,
    ref TEXT,
    sic TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE releases (
    release_id TEXT PRIMARY KEY,
    release TEXT,
    label TEXT REFERENCES labels(name),
    catno TEXT,
    format TEXT,
    market TEXT,
    label2 TEXT,
    va TEXT,
    labelX TEXT,
    nfo TEXT,
    catno2 TEXT,
    id2 TEXT,
    series TEXT,
    idX TEXT,
    special TEXT,
    extra TEXT
);

CREATE TABLE song_releases (
    song_id INTEGER NOT NULL,
    release_id TEXT NOT NULL,
    idX TEXT,
    disc TEXT,
    FOREIGN KEY (song_id) REFERENCES songs(song_id) ON DELETE CASCADE,
    FOREIGN KEY (release_id) REFERENCES releases(release_id) ON DELETE CASCADE,
    PRIMARY KEY (song_id, release_id)
);

CREATE TABLE compositions (
    title TEXT PRIMARY KEY,
    composer TEXT,
    copyright TEXT,
    style TEXT,
    sic TEXT
);

CREATE TABLE labels (
    name TEXT PRIMARY KEY,
    abr TEXT,
    markets TEXT,
    pernet_remark TEXT
);

CREATE TABLE countries (
    iso TEXT,
    pernet_abr TEXT PRIMARY KEY,
    pernet_adj TEXT,
    pernet_other TEXT,
    name TEXT
);
""")
conn.commit()


# Landen

root = parse_xml(XML_FILES['countries'])
if root is not None:
    for country in root.findall('.//country'):
        iso = country.get('ISO')
        pernet_abr = country.get('pernet_abr')
        pernet_adj = country.get('pernet_adj')
        pernet_other = country.get('pernet_other')
        name = country.text
        if name:
            c.execute('''INSERT OR IGNORE INTO countries
                         (iso, pernet_abr, pernet_adj, pernet_other, name)
                         VALUES (?,?,?,?,?)''',
                      (iso, pernet_abr, pernet_adj, pernet_other, name))
    conn.commit()
    print("Landen geladen.")


# Labels

root = parse_xml(XML_FILES['labels'])
if root is not None:
    for label in root.findall('.//label'):
        name = label.text
        abr = label.get('abr')
        markets = label.get('markets')
        pernet_remark = label.get('pernet_remark')
        if name:
            c.execute('INSERT OR IGNORE INTO labels (name, abr, markets, pernet_remark) VALUES (?,?,?,?)',
                      (name, abr, markets, pernet_remark))
    conn.commit()
    print("Labels geladen.")


# Composities

root = parse_xml(XML_FILES['compositions'])
if root is not None:
    for comp in root.findall('.//composition'):
        title = comp.text
        composer = comp.get('composer')
        copyright = comp.get('copyright')
        style = comp.get('style')
        sic = comp.get('sic')
        if title:
            c.execute('INSERT OR IGNORE INTO compositions (title, composer, copyright, style, sic) VALUES (?,?,?,?,?)',
                      (title, composer, copyright, style, sic))
    conn.commit()
    print("Composities geladen.")


# Releases

root = parse_xml(XML_FILES['releases'])
if root is not None:
    for release in root.findall('.//release'):
        release_id = release.get('id')
        release_text = release.text
        label = release.get('label')
        catno = release.get('catno')
        format = release.get('format')
        market = release.get('market')
        label2 = release.get('label2')
        va = release.get('va')
        labelX = release.get('labelX')
        nfo = release.get('nfo')
        catno2 = release.get('catno2')
        id2 = release.get('id2')
        series = release.get('series')
        idX = release.get('idX')
        special = release.get('special')
        extra = release.get('extra')
        if release_id:
            c.execute('''INSERT OR IGNORE INTO releases
                         (release_id, release, label, catno, format, market, label2, va, labelX,
                          nfo, catno2, id2, series, idX, special, extra)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                      (release_id, release_text, label, catno, format, market,
                       label2, va, labelX, nfo, catno2, id2, series, idX, special, extra))
    conn.commit()
    print("Releases geladen.")


# Artiesten (apart bestand)

root = parse_xml(XML_FILES['artists'])
if root is not None:
    for artist in root.findall('.//artist'):
        name = artist.text
        if name:
            c.execute('INSERT OR IGNORE INTO artists (name) VALUES (?)', (name,))
    conn.commit()
    print("Artiesten uit apart bestand geladen.")

# Sessies (subset)
root = parse_xml(XML_FILES['sessions'])
if root is None:
    print("Geen sessies gevonden. Script stopt.")
    conn.close()
    exit(1)

for session in root.findall('.//session'):
    session_id = session.get('id')
    if not session_id:
        continue
    summary = session.get('summary')
    pernet = session.get('pernet')          # attribuut op <session>
    
    # <artists> element
    artists_elem = session.find('artists')
    pernet_note = None
    band = None
    if artists_elem is not None:
        pernet_note = artists_elem.get('pernet')
        band = artists_elem.findtext('band')
    
    # Datum
    ymd = None
    year = month = day = None
    dates_elem = session.find('dates')
    if dates_elem is not None:
        ymd = dates_elem.get('ymd')
        date_elem = dates_elem.find('date')
        if date_elem is not None:
            year = date_elem.findtext('year')
            month = date_elem.findtext('month')
            day = date_elem.findtext('day')
    
    # Locatie
    locations_extra = None
    country = city = venue = None
    locations_elem = session.find('locations')
    if locations_elem is not None:
        locations_extra = locations_elem.get('extra')
        loc_elem = locations_elem.find('location')
        if loc_elem is not None:
            country = loc_elem.findtext('country')
            city = loc_elem.findtext('city')
            venue = loc_elem.get('venue')
    
    notes = session.findtext('notes')
    
    # Sessie invoegen
    c.execute('''INSERT OR IGNORE INTO sessions
                 (session_id, summary, pernet, pernet_note, band,
                  ymd, year, month, day, locations_extra,
                  country, city, venue, notes)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (session_id, summary, pernet, pernet_note, band,
               ymd, year, month, day, locations_extra,
               country, city, venue, notes))
    
    # Artiesten voor deze sessie
    if artists_elem is not None:
        for artist in artists_elem.findall('artist'):
            name = artist.text
            if not name:
                continue
            instr = artist.get('instr')
            info = artist.get('info')
            alias = artist.get('alias')
            # Zoek artist_id
            c.execute('SELECT artist_id FROM artists WHERE name = ?', (name,))
            row = c.fetchone()
            if row:
                artist_id = row[0]
                # session_artists record toevoegen
                c.execute('''INSERT OR IGNORE INTO session_artists
                             (session_id, artist_id, instruments, info, alias)
                             VALUES (?,?,?,?,?)''',
                          (session_id, artist_id, instr, info, alias))
    
    # Songs
    songs_elem = session.find('songs')
    if songs_elem is not None:
        for song in songs_elem.findall('song'):
            title = song.text
            compo = song.get('compo')
            mx = song.get('mx')
            take = song.get('take')
            credits = song.get('credits')
            ref = song.get('ref')
            sic = song.get('sic')
            c.execute('''INSERT INTO songs
                         (session_id, title, compo, mx, take, credits, ref, sic)
                         VALUES (?,?,?,?,?,?,?,?)''',
                      (session_id, title, compo, mx, take, credits, ref, sic))
            song_id = c.lastrowid
            
            # Releases voor deze song
            for rel in song.findall('releases/release'):
                release_id = rel.get('id')
                idX = rel.get('idX')
                disc = rel.get('disc')
                if release_id:
                    c.execute('INSERT OR IGNORE INTO song_releases (song_id, release_id, idX, disc) VALUES (?,?,?,?)',
                              (song_id, release_id, idX, disc))
            
            # Credits 
            credits_elem = song.find('credits')
            if credits_elem is not None:
                for credit in credits_elem.findall('credit'):
                    roll = credit.get('roll')
                    credit_text = credit.text
         
                    pass
            
            # Albums
            albums_elem = song.find('albums')
            if albums_elem is not None:
                for album in albums_elem.findall('album'):
                    ref_album = album.get('ref')
                    album_text = album.text
                    pass

conn.commit()
print("Sessies, songs en artiestkoppelingen verwerkt.")


# Instrumenten splitsen
c.execute("SELECT session_artist_id, instruments FROM session_artists WHERE instruments IS NOT NULL")
rows = c.fetchall()
instrument_count = 0
for sa_id, instr_str in rows:
    for instr in instr_str.split(','):
        instr = instr.strip()
        if not instr:
            continue
        c.execute("INSERT OR IGNORE INTO instruments (name) VALUES (?)", (instr,))
        c.execute("SELECT instrument_id FROM instruments WHERE name = ?", (instr,))
        instr_id = c.fetchone()[0]
        c.execute("INSERT OR IGNORE INTO session_artist_instruments (session_artist_id, instrument_id) VALUES (?, ?)",
                  (sa_id, instr_id))
        instrument_count += 1


conn.commit()
print(f"{instrument_count} instrumentkoppelingen toegevoegd.")

conn.close()
print(f"Database opgeslagen als {DB_FILE}")
