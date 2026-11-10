import argparse
import json
import time

from .events import simulate, validate
from .outbox import Outbox


def main():
    parser = argparse.ArgumentParser(description='Simulador e ingesta Kafka')
    parser.add_argument('command', choices=['topics', 'simulate', 'replay', 'consume'])
    parser.add_argument('--shipments', type=int, default=3)
    parser.add_argument('--steps', type=int, default=12, help='0: ejecución continua en ciclos')
    parser.add_argument('--interval', type=float, default=1)
    parser.add_argument('--scenario', choices=['normal', 'delay'], default='normal')
    parser.add_argument('--db', default='outbox.sqlite3')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--seconds', type=int, default=60)
    args = parser.parse_args()
    if args.shipments < 1 or args.steps < 0 or args.interval < 0 or args.limit < 1 or args.seconds < 1:
        parser.error('Valores fuera de rango')
    if args.command == 'topics':
        from .kafka_io import create_topics
        create_topics()
        return
    if args.command == 'consume':
        from .kafka_io import consume
        consume(args.limit, args.seconds)
        return
    box = None
    try:
        if not args.dry_run:
            from .kafka_io import KafkaSender
            box, sender = Outbox(args.db), KafkaSender()
            if not box.drain(sender):
                raise RuntimeError('Persisten pendientes. Recupera Kafka y ejecuta replay.')
        elif args.command == 'replay':
            parser.error('replay requiere Kafka')
        if args.command == 'simulate':
            while True:
                previous = 0
                for event, step in simulate(args.shipments, args.scenario, args.steps or 60):
                    if step != previous:
                        time.sleep(args.interval)
                        previous = step
                    validate(event)
                    if args.dry_run:
                        print(json.dumps(event, ensure_ascii=False))
                    else:
                        box.enqueue(event)
                        if not box.drain(sender):
                            raise RuntimeError('Evento guardado en outbox. Ejecuta replay al recuperar Kafka.')
                if args.steps:
                    break
        if box:
            print(f'Pendientes: {box.count()}')
    finally:
        if box:
            box.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrumpido; pendientes conservados en SQLite.')
