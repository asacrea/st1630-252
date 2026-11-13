import json
import sqlite3
import time

from .events import routing


class Outbox:
    """Un solo proceso escritor por archivo. Persiste antes de publicar."""
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS pending (seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE, topic TEXT, key TEXT, payload TEXT)')

    def enqueue(self, event):
        topic, key = routing(event)
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO pending(event_id,topic,key,payload) VALUES (?,?,?,?)',
                            (event['event_id'], topic, key, json.dumps(event, ensure_ascii=False)))

    def count(self):
        return self.db.execute('SELECT count(*) FROM pending').fetchone()[0]

    def drain(self, sender, attempts=3, backoff=.5):
        # Detenerse en el primer fallo conserva el orden de los eventos pendientes.
        while True:
            row = self.db.execute('SELECT seq,topic,key,payload FROM pending ORDER BY seq LIMIT 1').fetchone()
            if row is None:
                return True
            for attempt in range(attempts):
                try:
                    sender(row[1], row[2], row[3])
                    break
                except Exception as error:
                    print(f'Reintento {attempt+1}/{attempts}: {error}', flush=True)
                    if attempt + 1 == attempts:
                        return False
                    time.sleep(backoff * 2**attempt)
            with self.db:
                self.db.execute('DELETE FROM pending WHERE seq=?', (row[0],))

    def close(self):
        self.db.close()
