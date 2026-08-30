## productor_kafka.py:

Publicando 1000 pedidos en 'pedidos-ventas' (bootstrap: localhost:9092)...
  [100/1000] región=Bogotá       partición=0 offset=70
  [200/1000] región=Bucaramanga  partición=1 offset=48
  [300/1000] región=Medellín     partición=1 offset=78
  [400/1000] región=Bucaramanga  partición=1 offset=104
  [500/1000] región=Cali         partición=0 offset=282
  [600/1000] región=Bogotá       partición=0 offset=348
  [700/1000] región=Bucaramanga  partición=1 offset=183
  [800/1000] región=Bogotá       partición=0 offset=456
  [900/1000] región=Bogotá       partición=0 offset=503
  [1000/1000] región=Medellín     partición=1 offset=281

=== Resumen: región -> partición -> mensajes ===
  Bogotá         P0=400
  Medellín       P1=209
  Cali           P0=144
  Barranquilla   P3=97
  Bucaramanga    P1=73
  Otro           P2=77

## consumidor_kafka.py

Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

[OK] offset=15 partition=0 pedido_id=901                                        
[OK] offset=16 partition=0 pedido_id=903                                        
[OK] offset=17 partition=0 pedido_id=904                                        

Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.

21

Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

[OK] offset=17 partition=0 pedido_id=904                                        
[OK] offset=18 partition=0 pedido_id=999                                        

Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.

22