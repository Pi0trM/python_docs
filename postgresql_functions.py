"""
Moduł zawiera funkcje przeznaczone do pracy z bazą danych PostgreSQL.
"""

import json
import pandas as pd
import sql
import sqlalchemy as sa
import subprocess
import psycopg2
import os
import matplotlib.pyplot as plt

def get_connection_string(config_file = "database_creds.json"):
    """
    Zwraca connection string do połączenia z bazą PostgreSQL na podstawie pliku konfiguracyjnego.
    Używana w dalszych funkcjach.
    """
    with open(config_file, encoding="utf-8") as db_con_file:
        creds = json.load(db_con_file)

    return "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}".format(
        user=creds['user_name'],
        password=creds['password'],
        host=creds['host_name'],
        port=creds['port_number'],
        db_name=creds['db_name']
    )

def connect_to_db(config_file = "database_creds.json"):
    """
    Łączy się z bazą danych PostgreSQL za pośrednictwem danych zawartych w pliku database_creds.json.
    """

    connection_string = get_connection_string(config_file)
    
    print("Połączono z bazą.")

def table_to_csv(table, csv_file, config_file = "database_creds.json"):
    """
    Przepisuje zawartość tabeli z bazy danych PostgreSQL do pliku CSV.
    """
    connection_string = get_connection_string(config_file)
    
    engine = sa.create_engine(connection_string)

    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, engine)

    df.to_csv(csv_file, index = False, encoding = 'utf-8')

    print(f"Wyeksportowano dane z tabeli '{table}' do pliku '{csv_file}'.")
    return df.head()

def csv_to_table(table, csv_file, config_file = "database_creds.json"):
    """
    Prepisuje zawartość pliku CSV do tabeli w bazie PostgreSQL.
    Jeśli tabela o podanej nazwie już istnieje, zostanie zamieniona.
    """
    connection_string = get_connection_string(config_file)
    
    df = pd.read_csv(csv_file, dtype = {"PESEL": str})  # Zastosowanie typu string ma na celu zachowanie zer występujących na początku.

    engine = sa.create_engine(connection_string)

    df.to_sql(table, con=engine, if_exists = 'replace', index=False)

    print(f"Zaimportowano dane do tabeli '{table}'.")

def create_backup(backup_file = "backup.bak", config_file = "database_creds.json"):
    """
    Tworzy kopię bazy danych.
    """
    with open(config_file, encoding = "utf-8") as db_con_file:
        creds = json.load(db_con_file)
    
    command = [
        "pg_dump",
        "-h", creds["host_name"],
        "-p", str(creds["port_number"]),
        "-U", creds["user_name"],
        "-F", "c",
        "-b",
        "-f", backup_file,
        creds["db_name"]
    ]
    print("Tworzę backup PostgreSQL...")
    
    env = os.environ.copy()
    env["PGPASSWORD"] = creds["password"]
    
    result = subprocess.run(command, env=env)
    
    if result.returncode == 0:
        print(f"Kopia zapisana do pliku '{backup_file}'.")
    else:
        print("Błąd podczas tworzenia kopii zapasowej.")

def clear_db(tables = None, config_file = "database_creds.json"):
    """
    Czyści zawartość bazy danych.
    """
    connection_string = get_connection_string(config_file)
    
    engine = sa.create_engine(connection_string)
    meta = sa.MetaData()
    meta.reflect(bind = engine)

    with engine.begin() as conn:
        if tables == None:
            for table in reversed(meta.sorted_tables):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
            print("Baza danych została wyczyszczona.")
        else:
            for table in tables:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                print(f"Usunięto tabelę {table}.")

def normalize(config_file = "database_creds.json"):
    """
    Przepisuje dane z pierwotnej tabeli w pierwszym stopniu normalizacji,
    do nowych tabel w stopniu trzecim odpowiadających poszczególnym encjom,
    następnie kasuje tabelę pierwotną.
    """
    connection_string = get_connection_string(config_file)

    engine = sa.create_engine(connection_string)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS Kandydat (
                pesel CHAR(11) PRIMARY KEY,
                imie VARCHAR(100) NOT NULL,
                nazwisko VARCHAR(100) NOT NULL,
                kodpocztowy CHAR(6),
                telefon VARCHAR(20),
                sredniamaturalna NUMERIC(4, 2)
            );
        """))
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS Wydzial (
                idwydzialu SERIAL PRIMARY KEY,
                nazwawydzialu VARCHAR(255) NOT NULL
            );
        """))
        conn.execute(sa.text("""
            CREATE TABLE Aplikacja (
                pesel CHAR(11),
                idwydzialu INTEGER,
                datarekrutacji DATE NOT NULL,
                statusaplikacji VARCHAR(30),
            
                PRIMARY KEY (pesel),
                FOREIGN KEY (pesel) REFERENCES Kandydat(pesel) ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (idwydzialu) REFERENCES Wydzial(idwydzialu) ON DELETE RESTRICT ON UPDATE CASCADE
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
            SELECT pesel, idwydzialu, TO_DATE(datarekrutacji, 'YYYY-MM-DD'), statusaplikacji
            FROM kandydaci;
        """))

        conn.execute(sa.text("DROP TABLE kandydaci CASCADE;"))
        print("Normalizacja zakończona.")

def denormalize(config_file = "database_creds.json"):
    """
    Łączy trzy tabele w jedną tabelę o pierwszym stopniu normalizacji,
    następnie usuwa te tabele.
    """
    connection_string = get_connection_string(config_file)
    
    engine = sa.create_engine(connection_string)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS kandydaci AS
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
        
        conn.execute(sa.text("""
            DROP TABLE Kandydat CASCADE;
            DROP TABLE Wydzial CASCADE;
            DROP TABLE Aplikacja CASCADE;
        """))
    
    print("Denormalizacja zakończona.")

def generate_report(config_file = "database_creds.json", generate_image = False):
    """
    Generuje raport z bazy danych, podsumowuje liczbę kandydatów na wydziałach
    i średnią ocenę maturzystów.
    """
    connection_string = get_connection_string(config_file)
    
    engine = sa.create_engine(connection_string)

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
        plt.savefig('chart_postgresql.png', dpi=300, bbox_inches='tight')
    
    plt.show()

def search_candidates(config_file = "database_creds.json", wydzial = None, min_sred = None, status = None):
    """
    Wyszukuje kandydatów według kryteriów:
    - wydzial: nazwa wydziału,
    - min_sred: minimalna średnia maturalna,
    - status: status aplikacji.
    """
    connection_string = get_connection_string(config_file)
    
    engine = sa.create_engine(connection_string)

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

