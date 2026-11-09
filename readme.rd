# Métricas climáticas da Espanha com MapReduce (PySpark)

Calcula métricas climáticas por **região** e **ano** a partir de dados diários de estações meteorológicas da AEMET, usando o modelo **MapReduce** do Spark (`flatMap` → `reduceByKey`). O ambiente roda em **Docker**, então não é preciso instalar Java nem Spark no computador.

---

## 1. Estrutura do projeto

```
projeto/
├── main.py              # orquestra o Spark, executa as métricas e imprime os resumos
├── mapper.py            # funções de map (registro → (chave, valor))
├── reducer.py           # funções de reduce (combinam valores da mesma chave)
├── estaciones.csv       # estação → região
├── clima_espanha.csv    # dados diários das estações
├── requirements.txt     # dependências Python (pyspark)
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
└── salida_metricas/     # criada automaticamente com os resultados
```

Para executar:
cd caminho/para/projeto

Construa a imagem (só na primeira vez)
docker compose build

Repita os passos acima somente se alterar o `Dockerfile` ou o `requirements.txt`. Alterações em `main.py`, `mapper.py` e `reducer.py` **não** exigem rebuild, porque a pasta é montada como volume no container.

Execute as métricas
# todas as métricas
docker compose run --rm clima-spark

# uma métrica
docker compose run --rm clima-spark temperatura

# várias métricas
docker compose run --rm clima-spark temperatura records

Os resumos aparecem no terminal, e os resultados completos ficam em `salida_metricas/<métrica>/`.

---

## 4. Métricas disponíveis

Os resumos usam os anos completos **2023, 2024 e 2025** (constante `ANIOS_COMPLETOS` em `main.py`). Os arquivos em `salida_metricas/` contêm **todos** os anos.

| Métrica         | Pergunta que responde                                         | Colunas em `salida_metricas/<métrica>/part-*` (separadas por tab) |
|-----------------|---------------------------------------------------------------|---------------------------------------------------------------------|
| `lluvia`        | Quanto chove em cada região?                                  | região, ano, mm/dia médio, % de dias com chuva (≥ 1 mm), máximo diário (mm), nº obs |
| `dias_extremos` | Com que frequência faz calor extremo ou geada?                | região, ano, % dias com tmax ≥ 35 °C, % dias com tmin ≤ 0 °C, nº obs |
| `temperatura`   | Qual a temperatura média de cada região e da Espanha?         | região, ano, tmed, média das máximas, média das mínimas, soma tmed, soma tmax, soma tmin, nº obs |
| `records`       | Qual região atingiu a temperatura mais alta e a mais baixa, e quando? | região, ano, tmax recorde, data, estação, tmin recorde, data, estação |


## 5. Como funciona (MapReduce)

clima_espanha.csv
   │  sc.textFile + filtro do cabeçalho
   ▼
crear_mapper          → (data, estação, região, {temp_max, temp_min, precipitation})
   │  .cache()  (reaproveitado por todas as métricas)
   ▼
flatMap(map_X)        → ((região, ano), valor)
   ▼
reduceByKey(reduce_X) → ((região, ano), valor agregado)   [2 reducers]
   ▼
saveAsTextFile        → salida_metricas/X/part-0000*
   ▼
resumen_X             → lê os part-*, filtra 2023-2025 e imprime a tabela


## 6. Como adicionar uma nova métrica

1. **`mapper.py`**: crie `map_X(r)`, que recebe `r = (data, estação, região, valores)` e devolve `[(chave, valor)]`, ou `[]` para descartar o registro.
2. **`reducer.py`**: crie `reduce_X(a, b)` ou reaproveite `reduce_suma`. O reducer **precisa ser associativo e comutativo**, porque o Spark combina os valores em qualquer ordem:
   - para médias, acumule `(soma, contagem)` e divida só no final, nunca faça média de médias;
   - para máximo e mínimo com contexto, compare tuplas `(valor, data, estação)`.
3. **`main.py`**:
   - crie `metrica_X(registros)`, seguindo o modelo de `metrica_temperatura`;
   - crie `resumen_X()`, que lê com `leer("X")`;
   - registre a métrica em `METRICAS`: `"X": (metrica_X, resumen_X)`.
4. Execute: `docker compose run --rm clima-spark X`
