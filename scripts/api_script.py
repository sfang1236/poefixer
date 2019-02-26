import json
import logging
import argparse

import requests

import fixer
import fixer.logger as plogger


DEFAULT_DSN='sqlite:///:memory:'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--verbose', action='store_true', help='Verbose output')
    parser.add_argument(
        '--debug', action='store_true', help='Debugging output')
    parser.add_argument(
        '-d', '--database-dsn', action='store',
        default=DEFAULT_DSN,
        help='Database connection string for SQLAlchemy')
    parser.add_argument(
        '--most-recent', action='store_true',
        help='Consult poe.ninja to find latest ID')
    parser.add_argument(
        'next_id', action='store', nargs='?',
        help='The next id to start at')
    return parser.parse_args()

def pull_data(database_dsn, next_id, most_recent, logger):

    if most_recent:
        if next_id:
            raise ValueError("Cannot provide next_id with most-recent flag")
        result = requests.get('http://poe.ninja/api/Data/GetStats')
        result.raise_for_status()
        data = json.loads(result.text)
        next_id = data['next_change_id']

    db = fixer.PoeDb(db_connect=database_dsn, logger=logger)
    api = fixer.PoeApi(logger=logger, next_id=next_id)

    db.create_database()

    while True:
        for stash in api.get_next():
            logger.debug("Inserting stash...")
            db.insert_api_stash(stash, with_items=True)
        logger.info("Stash pass complete.")
        db.session.commit()


if __name__ == '__main__':
    options = parse_args()

    if options.debug:
        level = 'DEBUG'
    elif options.verbose:
        level = 'INFO'
    else:
        level = 'WARNING'
    logging.basicConfig(level=level)
    logger = plogger.get_fixer_logger(level)

    pull_data(
        database_dsn=options.database_dsn,
        next_id=options.next_id,
        most_recent=options.most_recent,
        logger=logger)