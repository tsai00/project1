from coosto import CoostoAPI
from ortec import OrtecAPI
from database import DatabaseConnection


if __name__ == '__main__':
    credentials_coosto = {'username': '*****', 'password': '*****'}
    results_path = 'export/'

    try:
        # Creating instances of API and database connection
        dbc = DatabaseConnection()
        ortec_api = OrtecAPI(results_path, '*****', '*****', match_id=75201, db_conn=dbc)
        coosto_api = CoostoAPI(results_path, **credentials_coosto, db_conn=dbc)

        # Export Ortec data
        ortec_api.players_stats_export(output='csv')
        ortec_api.teams_stats_export(output='csv')
        ortec_api.player_stats_meta_export(output='csv')
        ortec_api.team_stats_meta_export(output='csv')
        ortec_api.positions_meta_export(output='csv')
        ortec_api.venues_meta_export(output='csv')
        ortec_api.match_info_export(output='csv')  # For now 'replace' mode is used in order to eliminate errors with PK
        ortec_api.persons_export(output='csv', team_id=8326)
        ortec_api.teams_export(output='csv')
        ortec_api.export_team_lineups(output='csv', team_id=8326)

        # Set PK and FK for Ortec data
        ortec_api.set_pk_and_fk()

        #ortec_api.clean_database()

        # Export Coosto data
        coosto_api.export_all(output='csv')

        # Close database connection
        dbc.close_connection()

    except Exception as e:
        print(f'Error: {e.with_traceback()}')

    # Add file with settings for sql server

    # ADD LOGGING

    # COMMENT UNNECESSARY COLUMNS

    # LOGIC OF RUNNING SCRIPT (SCHEMA WITH UPDATE INTERVALS)

    # CONNECT INTERNAL DATA SOURCES


    # CREATE README.MD

    # First tables need to be created, then add PK and FK. Otherwise, some of tables may not be created yet
    # and FK could not be added








