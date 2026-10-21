"""Application click permettant la récupération des données de la sonde
météorologique depuis le début ou depuis la dernière TS enregistrée,
jusqu'à 00h00 du jour J.

CHANGED (data_structure_type support) : la position du bloc de relevés
météo dans la réponse de l'API dépend du `gateway_type` de la station.
La première version supposait que les données se trouvaient toujours dans
`data["sensors"][0]["data"]` ; avec le nouveau capteur, elles se trouvent
plus bas dans la liste `sensors`, sous un `data_structure_type`
correspondant à une entrée « Archive record »
(cf. https://weatherlink.github.io/v2-api/data-structure-types).
L'application détecte donc désormais le `data_structure_type` de chaque
capteur et stocke les données en conséquence : une table par type de
structure (`{table}_ds{type}`), plus une table `weatherlink_common`
regroupant les champs communs, une ligne par (station, horodatage)."""

# 1 : Librairies et options
import warnings
from importlib.metadata import metadata
from typing import Optional

import click

from .helpers import (  # CHANGED (data_structures.log) :
    echo_default,
    echo_failure,
    echo_info,
    echo_success,
    get_station_ids,
    get_stations_infos,
    last_ts_bdd,
    one_day_data,
    start_station,
    today_ts,
    up_to_bdd,
    write_data_structures_log,
)

# Ignorer les avertissements FutureWarning : colonnes 100% NaN
warnings.filterwarnings("ignore", category=FutureWarning)


# 4 : Utilisation de la routine de récupération des données via click :
@click.group()
# @click.option("--debug", default=False)
def cli():
    """Weatherlink2PG CLI app"""
    # echo_success(f"Debug mode is {'on' if debug else 'off'}")


@cli.command("version")
def get_version():
    """Get version"""

    app_metadata = metadata("weatherlink2pg")
    echo_default(app_metadata["name"])
    echo_info(f"version {app_metadata['Version']}")
    echo_info(app_metadata["Summary"])


@cli.command("stations")
def get_stations():
    """Get version"""
    get_stations_infos()


@cli.command("full")
@click.option(
    "--since",
    "-s",
    help="Date la plus ancienne à laquelle remonter (format AAAA-MM-JJ)",
)
@click.option("--station_id", "-i", help="ID de la station")
# CHANGED (archive_only) : option CLI pour ne conserver que les
# enregistrements « Archive record » :
@click.option(
    "--archive_only",
    "-a",
    is_flag=True,
    default=False,
    help="Ne conserver que les enregistrements « Archive record » "
    "(types de structure 3, 4, 7, 11, 13, 17, 20, 22, 24, 26)",
)
def full(
    since: Optional[str] = None,
    station_id: Optional[str] = None,
    archive_only: bool = False,
):
    """Commande de récupération des donnes depuis le début de la sonde
    avec une réinitialisation de la table."""
    echo_info("Lancement du script de téléchargement complet des données")
    if archive_only:
        # CHANGED (archive_only) :
        echo_info("Mode « Archive record » uniquement activé")

    stations = (
        get_station_ids()
        if not station_id
        else [
            station_id,
        ]
    )
    end_api = today_ts()
    first_day_station, if_exists_bdd = start_station(since)
    # CHANGED (data_structure_type support): one_day_data renvoie un
    # dictionnaire {data_structure_type: DataFrame}. up_to_bdd écrit
    # ensuite chaque type de structure de données dans sa propre table
    # `{table}_ds{type}`, ainsi qu'une table `weatherlink_common`
    # regroupant les champs communs à tous les types, avec une ligne par
    # (station_id, ts). La logique en amont est inchangée.
    df_new = one_day_data(stations, first_day_station, end_api, archive_only)
    # CHANGED (data_structures.log) : écriture de la structure JSON de
    # chaque data_structure_type unique rencontré, pour débogage :
    write_data_structures_log(df_new)
    up_to_bdd(df_new, if_exists_bdd)
    echo_success("Le script s'est exécuté avec succès.")


@cli.command("update")
@click.option("--station_id", "-i", help="ID de la station")
# CHANGED (archive_only) : option CLI pour ne conserver que les
# enregistrements « Archive record » :
@click.option(
    "--archive_only",
    "-a",
    is_flag=True,
    default=False,
    help="Ne conserver que les enregistrements « Archive record » "
    "(types de structure 3, 4, 7, 11, 13, 17, 20, 22, 24, 26)",
)
def update(
    station_id: Optional[str] = None,
    archive_only: bool = False,
):
    """Commande de mise à jour et d'ajout des données à la table."""

    echo_info("Lancement du script de mise à jour des données")
    if archive_only:
        # CHANGED (archive_only) :
        echo_info("Mode « Archive record » uniquement activé")
    stations = (
        get_station_ids()
        if not station_id
        else [
            station_id,
        ]
    )
    end_api = today_ts()
    last_ts, if_exists_bdd = last_ts_bdd()
    # CHANGED (data_structure_type support): correction du fallback
    # lorsque la base est encore vide (l'original référençait la fonction
    # start_station elle-même au lieu de sa valeur de retour).
    # last_ts_bdd renvoie None si aucune table `_ds*` ni `weatherlink_common`
    # n'existe encore ; dans ce cas on repart du début par défaut.
    # Le dictionnaire renvoyé par one_day_data est directement transmis à
    # up_to_bdd (tables par type + table commune `weatherlink_common`).
    if last_ts is None:
        last_ts, _ = start_station(None)
    df_new = one_day_data(stations, last_ts, end_api, archive_only)
    # CHANGED (data_structures.log) : écriture de la structure JSON de
    # chaque data_structure_type unique rencontré, pour débogage :
    write_data_structures_log(df_new)
    up_to_bdd(df_new, if_exists_bdd)
    echo_success("Le script s'est exécuté avec succès.")


if __name__ == "__main__":
    try:
        cli()
    except click.UsageError as e:
        echo_failure(f"Erreur d'utilisation : {e}")
    except click.Abort:
        echo_failure("L'opération a été interrompue.")
