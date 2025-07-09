"""
Moduł zawiera funkcje przeznaczone do pracy z bazą danych SQLite.
"""

import sqlite3
import pandas as pd
import json
import sqlalchemy as sa
import shutil
import matplotlib.pyplot as plt

def table_to_json(db_path, table, json_file):
    """
    Przepisuje zawartość tabeli z bazy danych SQLite do pliku JSON.
    """
    engine = sa.create_engine(f"sqlite:///{db_path}")

    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, engine)
    
    df.to_json(json_file, orient = 'records', force_ascii = False, indent = 4)
    
    print(f"Dane wyeksportowano do pliku '{json_file}'.")
    return df.head()

def json_to_table(db_path, table, json_file):
    """
    Prepisuje zawartość pliku JSON do tabeli w bazie SQLite.
    Jeśli tabela o podanej nazwie już istnieje, zostanie zamieniona.
    """
    with open(json_file, encoding = 'utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    engine = sa.create_engine(f"sqlite:///{db_path}")
    
    df.to_sql(table, con=engine, if_exists = 'replace', index = False)
    
    print(f"Dane zapisano do tabeli '{table}' w bazie danych.")

def create_backup(db_file, db_backup_file):
    """
    Tworzy kopię bazy danych.
    """
    shutil.copy2(db_file, db_backup_file)
    print(f"Kopia bazy '{db_file}' została zapisana jako '{db_backup_file}'.")

def clear_db(db_file, tables = None):
    """
    Czyści zawartość bazy danych.
    """
    engine = sa.create_engine(f"sqlite:///{db_file}")
    meta = sa.MetaData()
    meta.reflect(bind = engine)

    with engine.begin() as conn:
        if tables == None:
            for table in reversed(meta.sorted_tables):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table};"))
            print("Baza danych została wyczyszczona.")
        else:
            for table in tables:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table};"))
                print(f"Usunięto tabelę {table}.")

def normalize(db_file):
    """
    Przepisuje dane z pierwotnej tabeli w pierwszym stopniu normalizacji,
    do nowych tabel w stopniu trzecim odpowiadających poszczególnym encjom,
    następnie kasuje tabelę pierwotną.
    """
    engine = sa.create_engine(f"sqlite:///{db_file}")
    meta = sa.MetaData()
    meta.reflect(bind = engine)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS Kandydat (
                pesel TEXT PRIMARY KEY,
                imie TEXT NOT NULL,
                nazwisko TEXT NOT NULL,
                kodpocztowy TEXT,
                telefon TEXT,
                sredniamaturalna REAL
            );
        """))
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS Wydzial (
                idwydzialu INTEGER PRIMARY KEY,
                nazwawydzialu TEXT NOT NULL
            );
        """))
        conn.execute(sa.text("""
            CREATE TABLE Aplikacja (
                pesel TEXT,
                idwydzialu INTEGER,
                datarekrutacji TEXT NOT NULL,
                statusaplikacji TEXT,
            
                PRIMARY KEY (pesel),
                FOREIGN KEY (pesel) REFERENCES Kandydat(pesel),
                FOREIGN KEY (idwydzialu) REFERENCES Wydzial(idwydzialu)
            );
        """))
    
        conn.execute(sa.text("""
            INSERT INTO Kandydat (pesel, imie, nazwisko, kodpocztowy, telefon, sredniamaturalna)
            SELECT DISTINCT pesel, imie, nazwisko, kodpocztowy, telefon, sredniamaturalna
            FROM kandydaci;
        """))
        conn.execute(sa.text("""
            INSERT INTO Wydzial (idwydzialu, nazwawydzialu)
            SELECT DISTINCT idwydzialu, nazwawydzialu
            FROM kandydaci;
        """))
        conn.execute(sa.text("""
            INSERT INTO Aplikacja (pesel, idwydzialu, datarekrutacji, statusaplikacji)
            SELECT pesel, idwydzialu, datarekrutacji, statusaplikacji
            FROM kandydaci;
        """))

        conn.execute(sa.text("DROP TABLE kandydaci;"))
        print("Normalizacja zakończona.")

def denormalize(db_file):
    """
    Łączy trzy tabele w jedną tabelę o pierwszym stopniu normalizacji,
    następnie usuwa te tabele.
    """
    engine = sa.create_engine(f"sqlite:///{db_file}")
    meta = sa.MetaData()
    meta.reflect(bind = engine)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS kandydaci_denorm AS
            SELECT
                k.pesel,
                k.imie,
                k.nazwisko,
                k.kodpocztowy,
                k.telefon,
                k.sredniamaturalna,
                w.idwydzialu,
                w.nazwawydzialu,
                a.datarekrutacji,
                a.statusaplikacji
            FROM Kandydat k
            JOIN Aplikacja a ON k.pesel = a.pesel
            JOIN Wydzial w ON a.idwydzialu = w.idwydzialu;
        """))
        
        conn.execute(sa.text("DROP TABLE Kandydat;"))
        conn.execute(sa.text("DROP TABLE Wydzial;"))
        conn.execute(sa.text("DROP TABLE Aplikacja;"))
    
    print("Denormalizacja zakończona.")

def generate_report(db_file, generate_image = False):
    """
    Generuje raport z bazy danych, podsumowuje liczbę kandydatów na wydziałach
    i średnią ocenę maturzystów.
    """
    engine = sa.create_engine(f"sqlite:///{db_file}")

    query = """
    SELECT w.nazwawydzialu, COUNT(a.PESEL) AS liczba_kandydatow, AVG(k.sredniamaturalna) AS srednia_matura
    FROM Wydzial w
    JOIN Aplikacja a ON w.IDwydzialu = a.IDwydzialu
    JOIN Kandydat k ON a.PESEL = k.PESEL
    GROUP BY w.nazwawydzialu
    ORDER BY liczba_kandydatow DESC;
    """
    df = pd.read_sql(query, engine)

    print("\nPodsumowanie kandydatów na wydziałach:")
    print(df)

    df.plot(kind = "bar", x = "nazwawydzialu", y = "liczba_kandydatow", legend = False)
    plt.ylabel("Liczba kandydatów")
    plt.xlabel("Wydziały")
    plt.title("Liczba kandydatów na wydziałach")
    plt.tight_layout()

    if generate_image == True:
        plt.savefig('chart_sqlite.png', dpi=300, bbox_inches='tight')
    
    plt.show()

def search_candidates(db_file, wydzial = None, min_sred = None, status = None):
    """
    Wyszukuje kandydatów według kryteriów:
    - wydzial: nazwa wydziału,
    - min_sred: minimalna średnia maturalna,
    - status: status aplikacji.
    """
    engine = sa.create_engine(f"sqlite:///{db_file}")

    query = """
    SELECT k.PESEL, k.imie, k.nazwisko, k.sredniamaturalna, w.nazwawydzialu, a.statusaplikacji
    FROM Kandydat k
    JOIN Aplikacja a ON k.PESEL = a.PESEL
    JOIN Wydzial w ON a.IDwydzialu = w.IDwydzialu
    WHERE 1=1
    """

    params = {}
    if wydzial:
        query += " AND w.nazwawydzialu = :wydzial"
        params["wydzial"] = wydzial
    if min_sred:
        query += " AND k.sredniamaturalna >= :min_sred"
        params["min_sred"] = min_sred
    if status:
        query += " AND a.statusaplikacji = :status"
        params["status"] = status

    df = pd.read_sql(sa.text(query), engine, params = params)

    print("\nWyniki wyszukiwania:")
    print(df)
