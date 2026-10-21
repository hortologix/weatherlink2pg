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

# Paramètres de connexion à la base de données PostgreSQL en local :
host = os.getenv("WEATHERLINK2PG_PG_HOST", default="localhost")
database = os.getenv("WEATHERLINK2PG_PG_DATABASE", default="weatherlink")
port = os.getenv("WEATHERLINK2PG_PG_PORT", default="5432")
user = os.getenv("WEATHERLINK2PG_PG_USER")
password = os.getenv("WEATHERLINK2PG_PG_PWD")
table_name = os.getenv("WEATHERLINK2PG_PG_TABLE", default="data")
schema_name = os.getenv("WEATHERLINK2PG_PG_SCHEMA", default=None)
relation = f"{schema_name}.{table_name}" if schema_name else table_name

# Noms des tables créées par l'application :
COMMON_TABLE_NAME = "weatherlink_common"
FIELD_CATALOG_TABLE_NAME = "weatherlink_field_catalog"
SPECIFIC_TABLE_NAME = "weatherlink_specific"

# Types de structure correspondant à des « Archive record »
# (cf. https://weatherlink.github.io/v2-api/data-structure-types) :
#   3, 4  : WeatherLink IP / Vantage Connect Archive (Rev A / B)
#   7     : EnviroMonitor ISS Archive
#   11, 13 : WeatherLink Live ISS / non-ISS Archive
#   17    : AirLink Archive
#   20, 22, 24, 26 : WeatherLink Console Archive
# Les enregistrements « Current Conditions » et « Health » sont exclus.
ARCHIVE_TYPES = {3, 4, 7, 11, 13, 17, 20, 22, 24, 26}

# Colonnes techniques exclues des comparaisons de champs entre types :
EXCLUDED = {
    "station_id",
    "ts",
    "data_structure_type",
    "infos_json",
}

# Types SQL des tables écrites en base :
DTYPE_PER_TYPE = {
    "station_id": INTEGER,
    "ts": BIGINT,
    "data_structure_type": INTEGER,
    "infos_json": JSONB,
}
DTYPE_COMMON = {
    "station_id": INTEGER,
    "ts": BIGINT,
    "data_structure_type": INTEGER,
}
DTYPE_CATALOG = {
    "data_structure_type": INTEGER,
    "table_name": TEXT,
    "field_name": TEXT,
    "data_type": TEXT,
    "min_value": NUMERIC,
    "max_value": NUMERIC,
    "non_null_count": INTEGER,
    "total_count": INTEGER,
    "in_common_table": BOOLEAN,
}
DTYPE_SPECIFIC = {
    "station_id": INTEGER,
    "data_structure_type": INTEGER,
    "field_name": TEXT,
    "value_text": TEXT,
}


# 3 : Petits utilitaires partagés :
def _rel(table: str) -> str:
    """Nom qualifié schéma.table d'une relation, pour SQL et affichage."""
    return f"{schema_name or 'public'}.{table}"


def _query(sql: str, params=None, commit: bool = False) -> list[tuple]:
    """Exécute une requête SQL et renvoie les lignes résultat.

    Centralise la connexion psycopg2 (cursor, exécution, fermeture).
    """
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else []
        if commit:
            conn.commit()
        cur.close()
    finally:
        conn.close()
    return rows


def _api_get(url: str):
    """Requête GET authentifiée vers l'API Weatherlink ; JSON ou None."""
    headers = {"X-Api-Secret": API_secret}
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 200:
        return r.json()
    echo_failure(
        f"La requête {url} a échoué, code erreur :"
        f" {r.status_code} / {r.json()}"
    )
    return None


def _common_cols(df_ajout: dict[str, pd.DataFrame]) -> set:
    """Champs communs à tous les types, hors colonnes techniques."""
    if not df_ajout:
        return set()
    return (
        set.intersection(*[set(df.columns) for df in df_ajout.values()])
        - EXCLUDED
    )


def _concat_into(mapping: dict, key: str, df: pd.DataFrame) -> None:
    """Ajoute `df` à `mapping[key]` (concaténation si déjà présent)."""
    if key in mapping:
        mapping[key] = pd.concat([mapping[key], df], ignore_index=True)
    else:
        mapping[key] = df


def _make_echo(color: str):
    """Fabrique une fonction d'affichage coloré pour click."""

    def echo(message: str) -> None:
        """Affiche le message dans la couleur donnée."""
        text = message.replace("\n                     ", "")
        click.echo(click.style(text, fg=color))

    return echo


echo_success = _make_echo("green")
echo_info = _make_echo("blue")
echo_default = _make_echo("white")
echo_failure = _make_echo("red")


# 4 : Définitions :
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


def table_name_for(data_structure_type) -> str:
    """Nom de table dédié à un type de structure de données.

    Chaque `data_structure_type` est stocké dans sa propre table,
    nommée `{table_name}_ds{data_structure_type}` (ex: data_ds1).
    """
    dst = str(data_structure_type).replace("-", "_")
    return f"{table_name}_ds{dst}"


def data_structure_tables() -> list[str]:
    """Liste des tables créées pour les différents
    `data_structure_type` (tables `{table_name}_ds*`), incluant la table
    commune `weatherlink_common` si elle existe."""
    rows = _query(
        """SELECT table_name FROM information_schema.tables
           WHERE table_schema = %s
             AND (table_name LIKE %s OR table_name = %s)""",
        (schema_name or "public", f"{table_name}\\_ds%", COMMON_TABLE_NAME),
    )
    return [row[0] for row in rows]


def create_schema():
    """Create data schema if defined"""
    echo_success(f"CREATE SCHEMA {schema_name}")
    _query(f"CREATE SCHEMA IF NOT EXISTS {schema_name}", commit=True)


def last_ts_bdd():
    """Récupération de la dernière TS enregistrée dans la base de données.

    La dernière TS est la plus grande valeur trouvée dans l'ensemble des
    tables `{table_name}_ds*` et `weatherlink_common`.
    Si aucune table n'existe encore, renvoie (None, "append").
    """
    tables = data_structure_tables()
    if not tables:
        return None, "append"

    # Union des dernières TS de chaque table :
    parts = (
        f"SELECT ts FROM {_rel(t)} ORDER BY ts DESC LIMIT 1" for t in tables
    )
    union_query = " UNION ALL ".join(parts)
    rows = _query(f"SELECT max(ts) FROM ({union_query}) AS last_ts")
    last_ts = rows[0][0]

    return last_ts, "append"  # informations pour la BDD


def list_stations() -> Optional[list[dict]]:
    """Download stations main informations"""
    root_url = "https://api.weatherlink.com/v2/stations?"
    params = {
        "api-key": API_key,
    }
    url = root_url + urllib.parse.urlencode(params)

    data = _api_get(url)
    if data:
        return data["stations"]
    return None


def get_stations_infos() -> None:
    """Display stations main informations"""
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

    station_ids = [str(station["station_id"]) for station in stations]
    echo_info(f"Existing stations ids are {', '.join(station_ids)}")
    return station_ids


def fetch_station_data(station: int, start_time: int, end_time: int) -> dict:
    """Fetch data for a specific station and time range."""
    root_url = f"https://api.weatherlink.com/v2/historic/{station}?"
    params = {
        "api-key": API_key,
        "start-timestamp": start_time,
        "end-timestamp": end_time,
    }
    url = root_url + urllib.parse.urlencode(params)

    return _api_get(url)


def process_station_data(data: dict) -> dict[str, pd.DataFrame]:
    """Process the JSON data for a station into one DataFrame per
    data structure type.

    La première version de l'application supposait que les relevés
    météo se trouvaient toujours dans le premier bloc d'items
    (`data["sensors"][0]["data"]`), modèle valable pour l'ancien
    matériel. Or la position du bloc de relevés dépend du `gateway_type`
    de la station : avec le nouveau capteur, les données se trouvent
    plus bas dans la liste `sensors`, sous un `data_structure_type`
    correspondant à une entrée « Archive record »
    (cf. https://weatherlink.github.io/v2-api/data-structure-types).

    On ne peut donc pas se fier à la position : chaque capteur (sensor)
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
                # colonne traçant le type de structure de données :
                "data_structure_type": [data_structure_type] * len(records),
                "infos_json": records,
            }
        )
        df_sensors = pd.json_normalize(records)
        _concat_into(df_by_type, dst, pd.concat([df_jour, df_sensors], axis=1))

    return df_by_type


def one_day_data(
    stations: list[int],
    start_date_api: int,
    end_date_api: int,
    archive_only: bool = False,
) -> dict[str, pd.DataFrame]:
    """Récupération des données jour/jour via l'API et obtention d'une
    DataFrame par `data_structure_type`.

    Si `archive_only` vaut True, seuls les types de structure
    correspondant à des « Archive record » sont conservés ; les
    enregistrements « Current Conditions » et « Health » sont ignorés.
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
                # un DataFrame par data_structure_type :
                df_jour = process_station_data(data)
                for dst, df in df_jour.items():
                    if archive_only and int(dst) not in ARCHIVE_TYPES:
                        continue
                    _concat_into(df_ajout, dst, df)

    return df_ajout


def write_data_structures_log(
    df_ajout: dict[str, pd.DataFrame],
    log_path: str = "data_structures.log",
) -> None:
    """Écrit, à des fins de débogage, la structure JSON de chaque
    `data_structure_type` unique rencontré dans `data_structures.log`.

    Pour chaque type sont consignés :
      - le type de structure (`data_structure_type`) ;
      - la liste des champs (colonnes) du DataFrame associé, chaque
        champ commun à TOUS les types présents étant marqué d'un "*" ;
      - un enregistrement JSON complet de la colonne `infos_json`
        (première ligne) servant d'exemple de structure.
    """
    common_cols = _common_cols(df_ajout)

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(
            "Structures JSON par data_structure_type "
            f"({datetime.datetime.now().isoformat()})\n"
        )
        f.write('Un "*" devant un champ indique qu\'il est commun à ')
        f.write("tous les data_structure_type présents.\n")
        f.write("=" * 72 + "\n\n")
        for dst in sorted(df_ajout, key=int):
            df = df_ajout[dst]
            f.write(
                f"data_structure_type {dst} " f"({len(df)} enregistrements)\n"
            )
            f.write("-" * 72 + "\n")
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


def build_common_frame(df_ajout: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Construit la DataFrame des champs communs.

    - Les colonnes communes à TOUS les `data_structure_type` présents
      sont déterminées par intersection des colonnes des DataFrames.
    - Une seule ligne par (station_id, ts) : les valeurs des colonnes
      communes sont agrégées (première valeur non nulle) et
      `data_structure_type` conserve le type principal (le plus petit
      numéro). La liste complète des types par horodatage reste
      disponible dans les tables par type `{table}_ds{type}`.
    """
    if not df_ajout:
        return pd.DataFrame()

    common_cols = _common_cols(df_ajout)

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
    df_all["data_structure_type"] = df_all["data_structure_type"].astype(int)
    agg["data_structure_type"] = "min"

    df_common = (
        df_all.groupby(["station_id", "ts"], as_index=False)
        .agg(agg)
        .reset_index(drop=True)
    )
    return df_common


def verify_table_written(table: str, expected_rows: int) -> None:
    """Vérifie en base qu'une table existe et compte ses lignes, pour
    confirmer que l'écriture a bien abouti.

    Interroge information_schema (existence) puis COUNT(*) (lignes
    réellement présentes, pas une estimation).
    """
    exists = _query(
        """SELECT EXISTS (
               SELECT 1 FROM information_schema.tables
               WHERE table_schema = %s AND table_name = %s
           )""",
        (schema_name or "public", table),
    )[0][0]
    if not exists:
        echo_failure(
            f"VÉRIFICATION ÉCHOUÉE : la table {_rel(table)} "
            f"n'existe PAS dans la base '{database}' sur {host}:{port} !"
        )
    else:
        n = _query(f"SELECT COUNT(*) FROM {_rel(table)}")[0][0]
        echo_success(
            f"Vérifié : {_rel(table)} existe, {n} lignes en base "
            f"({expected_rows} lignes attendues)"
        )


def log_db_tables() -> None:
    """Affiche, en interrogeant directement la base, les tables du
    schéma cible et leur nombre de lignes.

    Permet de vérifier depuis les logs que les tables existent
    réellement dans la base où le script écrit.
    """
    rows = _query(
        """SELECT c.relname, c.reltuples::bigint
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = %s
             AND c.relkind = 'r'
           ORDER BY c.relname""",
        (schema_name or "public",),
    )

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


def build_field_catalog(df_ajout: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Construit un catalogue de champs par `data_structure_type`, pour
    comparer visuellement les champs de chaque type de structure.

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
    common_cols = _common_cols(df_ajout)

    rows = []
    for dst, df in df_ajout.items():
        for col in df.columns:
            s = df[col]
            row = {
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


def build_specific_frame(df_ajout: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Construit la DataFrame des valeurs NON communes, complément de
    `weatherlink_common`.

    Une ligne par combinaison unique (station_id,
    data_structure_type, field_name, value_text) : l'ensemble distinct
    des valeurs prises par chaque champ non commun, pour chaque station
    et chaque type de structure.
    """
    common_cols = _common_cols(df_ajout)

    frames = []
    for df in df_ajout.values():
        # Champs spécifiques à ce type = colonnes non communes :
        specific_cols = [
            c for c in df.columns if c not in common_cols and c not in EXCLUDED
        ]
        if not specific_cols:
            continue
        # Mise au format long : une ligne par (station, champ) ;
        # l'horodatage n'est pas conservé :
        df_long = df.melt(
            id_vars=["station_id", "data_structure_type"],
            value_vars=specific_cols,
            var_name="field_name",
            value_name="value_text",
        )
        df_long["value_text"] = df_long["value_text"].astype(str)
        # astype(str) transforme aussi les NaN en "nan" ; on nettoie :
        df_long.loc[df_long["value_text"] == "nan", "value_text"] = None
        # Dédoublonnage de la combinaison :
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


def _write_table(
    df: pd.DataFrame,
    table: str,
    engine,
    if_exists: str,
    dtype: dict,
) -> None:
    """Écrit une DataFrame dans une table puis vérifie en base."""
    echo_info(f"Writing {len(df)} rows to table {_rel(table)}")
    df.to_sql(
        table,
        engine,
        if_exists=if_exists,
        index=False,
        dtype=dtype,
        schema=schema_name,
    )
    verify_table_written(table, len(df))


def up_to_bdd(df_ajout: dict[str, pd.DataFrame], if_exists: str):
    """Ajout des données dans la BDD.

    `df_ajout` est un dictionnaire {data_structure_type: DataFrame}.
    Chaque type de structure de données est écrit dans sa propre table
    `{table_name}_ds{data_structure_type}`. S'y ajoutent :
      - `weatherlink_common` : champs communs à tous les types, une
        ligne par (station_id, ts) ;
      - `weatherlink_field_catalog` : catalogue des champs par type ;
      - `weatherlink_specific` : valeurs des champs non communs.
    """
    # Chaîne de connexion PostgreSQL :
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_str)

    echo_info(
        f"Connecting to PostgreSQL: host={host} port={port} "
        f"database='{database}' user='{user}' "
        f"schema='{schema_name or 'public'}'"
    )

    if schema_name is not None:
        create_schema()

    # Une table par data_structure_type :
    for dst, df in df_ajout.items():
        _write_table(
            df,
            table_name_for(dst),
            engine,
            if_exists,
            DTYPE_PER_TYPE,
        )

    # Table des champs communs :
    df_common = build_common_frame(df_ajout)
    if not df_common.empty:
        _write_table(
            df_common,
            COMMON_TABLE_NAME,
            engine,
            if_exists,
            DTYPE_COMMON,
        )

    # Catalogue des champs (comparaison visuelle des types) :
    df_catalog = build_field_catalog(df_ajout)
    if not df_catalog.empty:
        _write_table(
            df_catalog,
            FIELD_CATALOG_TABLE_NAME,
            engine,
            if_exists,
            DTYPE_CATALOG,
        )

    # Valeurs des champs non communs :
    df_specific = build_specific_frame(df_ajout)
    if not df_specific.empty:
        _write_table(
            df_specific,
            SPECIFIC_TABLE_NAME,
            engine,
            if_exists,
            DTYPE_SPECIFIC,
        )

    # Récapitulatif final depuis la base elle-même :
    log_db_tables()

    engine.dispose()
