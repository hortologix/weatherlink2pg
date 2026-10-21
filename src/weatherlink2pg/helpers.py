"""Définitions utilisées pour l'application click, clés API et BDD."""

# 1 : Librairies et options
import datetime
import json
import os
import urllib.parse
from typing import Optional

import click
import pandas as pd
import psycopg2
import requests
import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import (
    BIGINT,
    BOOLEAN,
    INTEGER,
    JSONB,
    NUMERIC,
    TEXT,
)

# 2 : Clés API et BDD via .env + url API
# Informations API : https://weatherlink.github.io/v2-api/

load_dotenv()

# Clés API :
API_key = os.getenv("WEATHERLINK2PG_API_KEY")
API_secret = os.getenv("WEATHERLINK2PG_API_SECRET")
# station_ID = os.getenv("WEATHERLINK2PG_API_STATIONID")

# Paramètres de connexion à la base de données PostgreSQL en local :
host = os.getenv("WEATHERLINK2PG_PG_HOST", default="localhost")
database = os.getenv("WEATHERLINK2PG_PG_DATABASE", default="weatherlink")
port = os.getenv("WEATHERLINK2PG_PG_PORT", default="5432")
user = os.getenv("WEATHERLINK2PG_PG_USER")
password = os.getenv("WEATHERLINK2PG_PG_PWD")
table_name = os.getenv("WEATHERLINK2PG_PG_TABLE", default="data")
schema_name = os.getenv("WEATHERLINK2PG_PG_SCHEMA", default=None)
relation = f"{schema_name}.{table_name}" if schema_name else table_name

# CHANGED (common table) : nom de la table fusionnée des champs communs.
COMMON_TABLE_NAME = "weatherlink_common"

# --- CHANGED (archive_only) -----------------------------------------------
# Types de structure correspondant à des « Archive record »
# (cf. https://weatherlink.github.io/v2-api/data-structure-types) :
#   3, 4  : WeatherLink IP / Vantage Connect Archive (Rev A / B)
#   7     : EnviroMonitor ISS Archive
#   11    : WeatherLink Live ISS Archive
#   13    : WeatherLink Live non-ISS Archive
#   17    : AirLink Archive
#   20, 22, 24, 26 : WeatherLink Console Archive
#   19    : (Barometer Archive, famille Console - conservé si utile)
# Les enregistrements « Current Conditions » et « Health » sont exclus.
ARCHIVE_TYPES = {3, 4, 7, 11, 13, 17, 20, 22, 24, 26}
# --- END CHANGED -----------------------------------------------------------


# 3 : Définitions :
def today_ts():
    """Récupération de la date du jour à 00h00 en TS pour utilisation comme
    date de fin avec l'API."""
    today = datetime.date.today()
    today_midnight = datetime.datetime.combine(today, datetime.time.min)
    end_date = int(today_midnight.timestamp())
    return end_date


def start_station(since: str):
    """Transformation de la date du début de la station en TS."""
    since = since if since else "2021-09-29"
    start_day = int(datetime.datetime.strptime(since, "%Y-%m-%d").timestamp())
    if_exists = "replace"  # informations pour la BDD
    return start_day, if_exists


# --- CHANGED (data_structure_type support) -------------------------------
def table_name_for(data_structure_type) -> str:
    """CHANGED: nom de table dédié à un type de structure de données.

    Chaque `data_structure_type` est stocké dans sa propre table,
    nommée `{table_name}_ds{data_structure_type}` (ex: data_ds1).
    """
    dst = str(data_structure_type).replace("-", "_")
    return f"{table_name}_ds{dst}"


def data_structure_tables() -> list[str]:
    """CHANGED: liste des tables créées pour les différents
    `data_structure_type` (tables `{table_name}_ds*`).
    CHANGED (common table) : inclut également la table commune
    `weatherlink_common` si elle existe."""
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()
    cur.execute(
        """SELECT table_name FROM information_schema.tables
           WHERE table_schema = %s
             AND (table_name LIKE %s OR table_name = %s)""",
        (schema_name or "public", f"{table_name}\\_ds%", COMMON_TABLE_NAME),
    )
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables


# --- END CHANGED ---------------------------------------------------------


def create_schema():
    """Create data schema if defined"""
    echo_success(f"CREATE SCHEMA {schema_name}")
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()

    # Exécution d'une requête SQL et récupération de la TS :
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.commit()
    # Fermeture du curseur et de la connexion
    cur.close()
    conn.close()


def last_ts_bdd():
    """Récupération de la dernière TS enregistrée dans la base de données.

    CHANGED: la dernière TS est la plus grande valeur trouvée dans
    l'ensemble des tables `{table_name}_ds*` et `weatherlink_common`.
    Si aucune table n'existe encore, renvoie (None, "append").
    """
    tables = data_structure_tables()
    if not tables:
        return None, "append"

    # Connexion à la base de données
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()
    # CHANGED: union des dernières TS de chaque table :
    union_query = " UNION ALL ".join(
        f"""SELECT ts FROM {f'{schema_name}.{t}' if schema_name else t} """
        "ORDER BY ts DESC LIMIT 1"
        for t in tables
    )
    cur.execute(f"SELECT max(ts) FROM ({union_query}) AS last_ts")
    last_ts = cur.fetchone()[0]

    if_exists = "append"  # informations pour la BDD

    # Fermeture du curseur et de la connexion
    cur.close()
    conn.close()

    return last_ts, if_exists


def list_stations() -> Optional[list[dict]]:
    """Download stations main informations"""
    root_url = "https://api.weatherlink.com/v2/stations?"
    params = {
        "api-key": API_key,
    }
    url = root_url + urllib.parse.urlencode(params)

    headers = {"X-Api-Secret": API_secret}

    # Requête :
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 200:
        data = r.json()
        return data["stations"]
    echo_failure(
        f"La requête {url} a échoué, code erreur :"
        f" {r.status_code} / {r.json()}"
    )
    return None


def get_stations_infos() -> None:
    "Display stations main informations"
    stations = list_stations()
    output = [
        f"""ID: {s["station_id"]} - NAME : {s['station_name']} """
        f"""({s['city']} - {s['longitude']}/{s['latitude']})
"""
        for s in stations
    ]
    echo_info(str("\n".join(output)))


def get_station_ids() -> Optional[list[int]]:
    """Get available stations id"""

    stations = list_stations()

    # Requête :
    station_ids = [str(station["station_id"]) for station in stations]
    echo_info(f"Existing stations ids are {', '.join(station_ids)}")
    return station_ids


def fetch_station_data(
    station: int, start_time: int, end_time: int
) -> pd.DataFrame:
    """Fetch data for a specific station and time range."""
    root_url = f"https://api.weatherlink.com/v2/historic/{station}?"
    params = {
        "api-key": API_key,
        "start-timestamp": start_time,
        "end-timestamp": end_time,
    }
    url = root_url + urllib.parse.urlencode(params)
    headers = {"X-Api-Secret": API_secret}

    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 200:
        return r.json()
    echo_failure(
        f"La requête {url} a échoué, code erreur :"
        f" {r.status_code} / {r.json()}"
    )
    return None


def process_station_data(data: dict) -> dict[str, pd.DataFrame]:
    """Process the JSON data for a station into one DataFrame per
    data structure type.

    CHANGED (data_structure_type support) : la première version de
    l'application supposait que les relevés météo se trouvaient toujours
    dans le premier bloc d'items (`data["sensors"][0]["data"]`), modèle
    valable pour l'ancien matériel. Or la position du bloc de relevés
    dépend du `gateway_type` de la station : avec le nouveau capteur,
    les données se trouvent plus bas dans la liste `sensors`, sous un
    `data_structure_type` correspondant à une entrée « Archive record »
    (cf. https://weatherlink.github.io/v2-api/data-structure-types).

    On ne peut donc plus se fier à la position : chaque capteur (sensor)
    est parcouru et son `data_structure_type` est détecté. Le
    dictionnaire renvoyé associe chaque `data_structure_type` à un
    DataFrame contenant les données de tous les capteurs de ce type.
    """
    df_by_type: dict[str, pd.DataFrame] = {}

    for sensor in data.get("sensors", []):
        data_structure_type = sensor.get("data_structure_type")
        if data_structure_type is None:
            echo_failure(
                "Capteur sans `data_structure_type`, données ignorées"
                f" (station {data.get('station_id')})."
            )
            continue

        records = sensor.get("data") or []
        if not records:
            continue

        dst = str(data_structure_type)
        df_jour = pd.DataFrame(
            {
                "station_id": [data["station_id"]] * len(records),
                # CHANGED: colonne traçant le type de structure de données :
                "data_structure_type": [data_structure_type] * len(records),
                "infos_json": records,
            }
        )
        df_sensors = pd.json_normalize(records)
        df_sensor_data = pd.concat([df_jour, df_sensors], axis=1)

        if dst in df_by_type:
            df_by_type[dst] = pd.concat(
                [df_by_type[dst], df_sensor_data], ignore_index=True
            )
        else:
            df_by_type[dst] = df_sensor_data

    return df_by_type


def one_day_data(
    stations: list[int],
    start_date_api: int,
    end_date_api: int,
    archive_only: bool = False,
) -> dict[str, pd.DataFrame]:
    """Récupération des données jour/jour via l'API et optention d'une DF.

    CHANGED (data_structure_type support): renvoie un dictionnaire
    associant chaque `data_structure_type` à un DataFrame.

    CHANGED (archive_only): si `archive_only` vaut True, seuls les types
    de structure correspondant à des « Archive record » sont conservés
    (cf. https://weatherlink.github.io/v2-api/data-structure-types) ;
    les enregistrements « Current Conditions » et « Health » sont
    ignorés.
    """
    df_ajout: dict[str, pd.DataFrame] = {}
    nb_jours = int((end_date_api - start_date_api) / 86400)

    for station in stations:
        echo_info(f"Downloading data from station {station}")
        for i in tqdm.tqdm(range(nb_jours)):
            start_time = start_date_api + i * 86400
            end_time = start_time + 86400

            data = fetch_station_data(station, start_time, end_time)
            if data:
                # CHANGED: un DataFrame par data_structure_type :
                df_jour = process_station_data(data)
                for dst, df in df_jour.items():
                    # CHANGED (archive_only) : filtre des types « Archive
                    # record » uniquement (Current Conditions et Health
                    # exclus) :
                    if archive_only and int(dst) not in ARCHIVE_TYPES:
                        continue
                    if dst in df_ajout:
                        df_ajout[dst] = pd.concat(
                            [df_ajout[dst], df], ignore_index=True
                        )
                    else:
                        df_ajout[dst] = df

    return df_ajout


# --- CHANGED (data_structures.log) ----------------------------------------
def write_data_structures_log(
    df_ajout: dict[str, pd.DataFrame],
    log_path: str = "data_structures.log",
) -> None:
    """CHANGED (data_structures.log) : écrit, à des fins de débogage, la
    structure JSON de chaque `data_structure_type` unique rencontré
    dans le fichier `data_structures.log`.

    Pour chaque type sont consignés :
      - le type de structure (`data_structure_type`) ;
      - la liste des champs (colonnes) du DataFrame associé ;
      - un enregistrement JSON complet de la colonne `infos_json`
        (première ligne) servant d'exemple de structure.

    CHANGED (marquage des champs communs) : dans la liste des champs,
    chaque champ commun à TOUS les `data_structure_type` présents est
    marqué d'un "*" (les colonnes techniques station_id, ts,
    data_structure_type et infos_json sont exclues de ce calcul).
    """
    # CHANGED (marquage des champs communs) : intersection des colonnes
    # de tous les types, hors colonnes techniques :
    excluded = {"station_id", "ts", "data_structure_type", "infos_json"}
    if df_ajout:
        common_cols = (
            set.intersection(*[set(df.columns) for df in df_ajout.values()])
            - excluded
        )
    else:
        common_cols = set()

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(
            "Structures JSON par data_structure_type "
            f"({datetime.datetime.now().isoformat()})\n"
        )
        # CHANGED (marquage des champs communs) : légende :
        f.write('Un "*" devant un champ indique qu\'il est commun à ')
        f.write("tous les data_structure_type présents.\n")
        f.write("=" * 72 + "\n\n")
        for dst in sorted(df_ajout, key=int):
            df = df_ajout[dst]
            f.write(
                f"data_structure_type {dst} " f"({len(df)} enregistrements)\n"
            )
            f.write("-" * 72 + "\n")
            # CHANGED (marquage des champs communs) : "*" devant chaque
            # champ présent dans l'intersection de tous les types :
            f.write(
                "Champs (colonnes) : "
                + ", ".join(
                    f"{col}*" if col in common_cols else str(col)
                    for col in df.columns
                )
                + "\n"
            )
            # Exemple d'enregistrement JSON brut depuis infos_json :
            if "infos_json" in df.columns and len(df) > 0:
                f.write("Exemple d'enregistrement (infos_json, 1re ligne) :\n")
                f.write(
                    json.dumps(
                        df["infos_json"].iloc[0],
                        indent=2,
                        ensure_ascii=False,
                        default=str,
                    )
                )
                f.write("\n")
            f.write("\n")
    echo_info(f"Structure JSON des types écrits dans {log_path}")


# --- END CHANGED -----------------------------------------------------------


# --- CHANGED (common table) ----------------------------------------------
def build_common_frame(df_ajout: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """CHANGED (common table) : construit la DataFrame des champs communs.

    - Les colonnes communes à TOUS les `data_structure_type` présents
      sont déterminées par intersection des colonnes des DataFrames.
    - Une seule ligne par (station_id, ts) : les valeurs des colonnes
      communes sont agrégées (première valeur non nulle), et la colonne
      `data_structure_type` contient la liste des types ayant produit un
      enregistrement pour ce couple station/horodatage (ex: "1;11").
    - Les colonnes `infos_json` et techniques sont exclues de la partie
      "champs météo communs" (elles restent disponibles dans les tables
      par type).
    """
    if not df_ajout:
        return pd.DataFrame()

    # Colonnes exclues de l'intersection des champs météo :
    excluded = {
        "station_id",
        "ts",
        "data_structure_type",
        "infos_json",
    }

    # Intersection des colonnes de tous les types de structure :
    common_cols = (
        set.intersection(*[set(df.columns) for df in df_ajout.values()])
        - excluded
    )

    frames = []
    for df in df_ajout.values():
        frames.append(
            df[
                ["station_id", "ts", "data_structure_type"]
                + sorted(common_cols)
            ].copy()
        )
    df_all = pd.concat(frames, ignore_index=True)

    # Une ligne par (station_id, ts) :
    agg = {col: "first" for col in sorted(common_cols)}
    # CHANGED (int data_structure_type) : une ligne par (station_id, ts),
    # avec data_structure_type en INTEGER. Si plusieurs types de structure
    # ont produit un enregistrement pour le même couple station/horodatage,
    # on conserve le type principal (le plus petit numéro). La liste
    # complète des types par horodatage reste disponible dans les tables
    # par type `{table}_ds{type}`.
    df_all["data_structure_type"] = df_all["data_structure_type"].astype(int)
    agg["data_structure_type"] = "min"

    df_common = (
        df_all.groupby(["station_id", "ts"], as_index=False)
        .agg(agg)
        .reset_index(drop=True)
    )
    return df_common


def verify_table_written(table: str, expected_rows: int) -> None:
    """CHANGED (logging) : vérifie en base qu'une table existe et compte
    ses lignes, pour confirmer que l'écriture a bien abouti.

    Interroge information_schema (existence) puis COUNT(*) (lignes
    réellement présentes, pas une estimation).
    """
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()
    cur.execute(
        """SELECT EXISTS (
               SELECT 1 FROM information_schema.tables
               WHERE table_schema = %s AND table_name = %s
           )""",
        (schema_name or "public", table),
    )
    exists = cur.fetchone()[0]
    if not exists:
        echo_failure(
            f"VÉRIFICATION ÉCHOUÉE : la table "
            f"{schema_name + '.' if schema_name else ''}{table} "
            f"n'existe PAS dans la base '{database}' sur {host}:{port} !"
        )
    else:
        schema = schema_name or "public"
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        n = cur.fetchone()[0]
        echo_success(
            f"Vérifié : {schema_name + '.' if schema_name else ''}{table} "
            f"existe, {n} lignes en base "
            f"({expected_rows} lignes attendues)"
        )
    cur.close()
    conn.close()


def log_db_tables() -> None:
    """CHANGED (logging) : affiche, en interrogeant directement la base,
    les tables du schéma cible et leur nombre de lignes.

    Permet de vérifier depuis les logs que les tables existent réellement
    dans la base où le script écrit (et pas seulement que `to_sql` n'a
    pas levé d'erreur).
    """
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()
    cur.execute(
        """SELECT c.relname, c.reltuples::bigint
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = %s
             AND c.relkind = 'r'
           ORDER BY c.relname""",
        (schema_name or "public",),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        echo_failure(
            f"Aucune table trouvée dans le schéma "
            f"{schema_name or 'public'} de la base {database} "
            f"sur {host}:{port} !"
        )
        return
    echo_info(
        f"Tables présentes dans {schema_name or 'public'} "
        f"(base '{database}' sur {host}:{port}) :"
    )
    for relname, n_rows in rows:
        # reltuples est une estimation (pas exacte juste après écriture) :
        echo_info(f"  - {relname} (~{n_rows} lignes)")


# --- CHANGED (field catalog) ---------------------------------------------
FIELD_CATALOG_TABLE_NAME = "weatherlink_field_catalog"

# CHANGED (specific table) : nom de la table des champs NON communs.
SPECIFIC_TABLE_NAME = "weatherlink_specific"


def build_field_catalog(df_ajout: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """CHANGED (field catalog) : construit un catalogue de champs par
    data_structure_type, pour comparer visuellement les champs de chaque
    type de structure.

    Une ligne par (data_structure_type, champ) avec :
      - data_structure_type : type de structure de données
      - table_name          : table de stockage ({table}_ds{type})
      - field_name          : nom du champ
      - data_type           : type pandas de la colonne
      - min_value / max_value : plage de valeurs numériques (NULL si non
                                numérique)
      - non_null_count      : nombre de valeurs non nulles
      - total_count         : nombre total d'enregistrements du type
      - in_common_table     : le champ fait-il partie des champs communs
                              écrits dans `weatherlink_common` ?
    """
    excluded = {
        "station_id",
        "ts",
        "data_structure_type",
        "infos_json",
    }
    # Champs communs = intersection des colonnes de tous les types :
    if df_ajout:
        common_cols = (
            set.intersection(*[set(df.columns) for df in df_ajout.values()])
            - excluded
        )
    else:
        common_cols = set()

    rows = []
    for dst, df in df_ajout.items():
        for col in df.columns:
            s = df[col]
            row = {
                # CHANGED (int data_structure_type) : cast en entier
                # (les clés du dictionnaire sont des chaînes) :
                "data_structure_type": int(dst),
                "table_name": table_name_for(dst),
                "field_name": col,
                "data_type": str(s.dtype),
                "min_value": None,
                "max_value": None,
                "non_null_count": int(s.count()),
                "total_count": int(len(s)),
                "in_common_table": bool(col in common_cols),
            }
            # Plage de valeurs uniquement pour les colonnes numériques :
            if pd.api.types.is_numeric_dtype(s):
                row["min_value"] = s.min()
                row["max_value"] = s.max()
            rows.append(row)

    return pd.DataFrame(rows)


# --- END CHANGED ---------------------------------------------------------


# --- CHANGED (specific table) --------------------------------------------
def build_specific_frame(df_ajout: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """CHANGED (specific table) : construit la DataFrame des valeurs NON
    communes, en format « long », complément de `weatherlink_common`.

    CHANGED (unique combos) : sans l'horodatage — une ligne par
    combinaison unique (station_id, data_structure_type, field_name,
    value_text), c'est-à-dire l'ensemble distinct des valeurs prises par
    chaque champ non commun pour chaque station et type de structure.
    """
    excluded = {
        "station_id",
        "ts",
        "data_structure_type",
        "infos_json",
    }
    # Champs communs = intersection des colonnes de tous les types :
    if df_ajout:
        common_cols = (
            set.intersection(*[set(df.columns) for df in df_ajout.values()])
            - excluded
        )
    else:
        common_cols = set()

    frames = []
    for df in df_ajout.values():
        # Champs spécifiques à ce type = colonnes non communes :
        specific_cols = [
            c for c in df.columns if c not in common_cols and c not in excluded
        ]
        if not specific_cols:
            continue
        # Mise au format long : une ligne par (station, ts, champ) ;
        # l'horodatage n'est pas conservé (voir dédoublonnage ci-dessous) :
        df_long = df.melt(
            id_vars=["station_id", "data_structure_type"],
            value_vars=specific_cols,
            var_name="field_name",
            value_name="value_text",
        )
        df_long["value_text"] = df_long["value_text"].astype(str)
        # astype(str) transforme aussi les NaN en "nan" ; on nettoie :
        df_long.loc[df_long["value_text"] == "nan", "value_text"] = None
        # CHANGED (unique combos) : dédoublonnage sur la combinaison
        # (station_id, data_structure_type, field_name, value_text) :
        df_long = df_long.drop_duplicates(
            subset=[
                "station_id",
                "data_structure_type",
                "field_name",
                "value_text",
            ]
        )
        frames.append(df_long)

    if not frames:
        return pd.DataFrame(
            columns=[
                "station_id",
                "data_structure_type",
                "field_name",
                "value_text",
            ]
        )
    return pd.concat(frames, ignore_index=True)


# --- END CHANGED ---------------------------------------------------------


def up_to_bdd(df_ajout: dict[str, pd.DataFrame], if_exists: str):
    """Ajout des données dans la BDD.

    CHANGED (data_structure_type support): `df_ajout` est un dictionnaire
    {data_structure_type: DataFrame}. Chaque type de structure de données
    est écrit dans sa propre table `{table_name}_ds{data_structure_type}`.

    CHANGED (common table): en plus des tables par type, une table
    `weatherlink_common` est écrite : une ligne par (station_id, ts)
    contenant les champs communs à tous les types de structure ainsi que
    la liste des `data_structure_type` correspondants.
    """
    # Connexion de la chaîne de connexion PostgreSQL :
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_str)

    # CHANGED (logging) : affiche explicitement où le script écrit :
    echo_info(
        f"Connecting to PostgreSQL: host={host} port={port} "
        f"database='{database}' user='{user}' "
        f"schema='{schema_name or 'public'}'"
    )

    # Définir les types de données pour chaque colonne :
    # CHANGED: ajout de la colonne data_structure_type :
    dtype = {
        "station_id": INTEGER,
        "ts": BIGINT,
        "data_structure_type": INTEGER,
        "infos_json": JSONB,
    }
    if schema_name is not None:
        create_schema()

    # CHANGED: une table par data_structure_type :
    for dst, df in df_ajout.items():
        dst_table = table_name_for(dst)
        echo_info(
            f"Writing {len(df)} rows of data_structure_type {dst} "
            f"to table {schema_name + '.' if schema_name else ''}{dst_table}"
        )
        df.to_sql(
            dst_table,
            engine,
            if_exists=if_exists,
            index=False,
            dtype=dtype,
            schema=schema_name,
        )
        # CHANGED (logging) : vérification en base après chaque écriture :
        verify_table_written(dst_table, len(df))

    # CHANGED (common table): écriture de la table des champs communs :
    df_common = build_common_frame(df_ajout)
    if not df_common.empty:
        echo_info(
            f"Writing {len(df_common)} rows to table "
            f"{schema_name + '.' if schema_name else ''}{COMMON_TABLE_NAME}"
        )
        df_common.to_sql(
            COMMON_TABLE_NAME,
            engine,
            if_exists=if_exists,
            index=False,
            dtype={
                "station_id": INTEGER,
                "ts": BIGINT,
                # CHANGED (int data_structure_type) : TEXT -> INTEGER :
                "data_structure_type": INTEGER,
            },
            schema=schema_name,
        )
        # CHANGED (logging) : vérification en base après chaque écriture :
        verify_table_written(COMMON_TABLE_NAME, len(df_common))

    # CHANGED (field catalog) : écriture du catalogue de champs, une
    # ligne par (data_structure_type, champ) avec type, plage de valeurs
    # et appartenance à la table commune -> permet une comparaison
    # visuelle des champs de chaque type de structure.
    df_catalog = build_field_catalog(df_ajout)
    if not df_catalog.empty:
        echo_info(
            f"Writing {len(df_catalog)} field descriptions to table "
            f"{schema_name + '.' if schema_name else ''}"
            f"{FIELD_CATALOG_TABLE_NAME}"
        )
        df_catalog.to_sql(
            FIELD_CATALOG_TABLE_NAME,
            engine,
            if_exists=if_exists,
            index=False,
            dtype={
                # CHANGED (int data_structure_type) : TEXT -> INTEGER :
                "data_structure_type": INTEGER,
                "table_name": TEXT,
                "field_name": TEXT,
                "data_type": TEXT,
                "min_value": NUMERIC,
                "max_value": NUMERIC,
                "non_null_count": INTEGER,
                "total_count": INTEGER,
                "in_common_table": BOOLEAN,
            },
            schema=schema_name,
        )
        # CHANGED (logging) : vérification en base après chaque écriture :
        verify_table_written(FIELD_CATALOG_TABLE_NAME, len(df_catalog))

    # CHANGED (specific table) : écriture de la table des valeurs NON
    # communes, dédoublonnées — une ligne par combinaison unique
    # (station_id, data_structure_type, field_name, value_text),
    # complément de `weatherlink_common`.
    df_specific = build_specific_frame(df_ajout)
    if not df_specific.empty:
        echo_info(
            f"Writing {len(df_specific)} unique non-common field values "
            f"to table "
            f"{schema_name + '.' if schema_name else ''}{SPECIFIC_TABLE_NAME}"
        )
        df_specific.to_sql(
            SPECIFIC_TABLE_NAME,
            engine,
            if_exists=if_exists,
            index=False,
            dtype={
                "station_id": INTEGER,
                "data_structure_type": INTEGER,
                "field_name": TEXT,
                "value_text": TEXT,
            },
            schema=schema_name,
        )
        # CHANGED (logging) : vérification en base après chaque écriture :
        verify_table_written(SPECIFIC_TABLE_NAME, len(df_specific))

    # CHANGED (logging) : récapitulatif final depuis la base elle-même :
    log_db_tables()

    # Fermeture de la connexion :
    engine.dispose()


# --- END CHANGED ---------------------------------------------------------


def echo_success(message):
    """Decore pour le succes du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="green",
        )
    )


def echo_info(message):
    """Decore pour le succes du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="blue",
        )
    )


def echo_default(message):
    """Decore pour le succes du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="white",
        )
    )


def echo_failure(message):
    """Décore en cas d'échéc du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="red",
        )
    )
