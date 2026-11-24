"""Independent synthetic provider process; private SQLite ledger, no shared DB transaction."""
import json
import sqlite3
import sys
from contextlib import closing

path, action, operation = sys.argv[1:]
if not operation.startswith('synthetic-'):
    raise ValueError('Synthetic IDs only')
with closing(sqlite3.connect(path)) as db:
    db.execute('CREATE TABLE IF NOT EXISTS charges (operation TEXT PRIMARY KEY, amount INTEGER NOT NULL)')
    if action == 'charge':
        # This commit is entirely independent of DynamoDB and precedes the response.
        db.execute('INSERT OR IGNORE INTO charges VALUES (?, ?)', (operation, 1000))
        db.commit()
    elif action != 'lookup':
        raise ValueError('Unknown action')
    row = db.execute('SELECT operation, amount FROM charges WHERE operation=?', (operation,)).fetchone()
    print(json.dumps({'charge_count': int(row is not None), 'operation_id': operation,
                      'amount_minor': row[1] if row else None}))
