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

### Formato esperado dos dados

**`clima_espanha.csv`**: separado por vírgula, com cabeçalho. As colunas usadas são:

| coluna          | conteúdo                            |
|-----------------|-------------------------------------|
| `date`          | data no formato `AAAA-MM-DD`        |
| `site`          | identificador da estação            |
| `temp_max`      | temperatura máxima do dia (°C)      |
| `temp_min`      | temperatura mínima do dia (°C)      |
| `precipitation` | precipitação do dia (mm)            |

A ordem das colunas não importa, porque o código localiza cada uma pelo nome no cabeçalho. Valores vazios ou inválidos são ignorados.

**`estaciones.csv`**: com cabeçalho e as colunas `site` e `region`. Só as estações listadas aqui entram no cálculo.

---

## 2. Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e **aberto**
- Um `requirements.txt` com o PySpark:

  ```
  pyspark>=4.0
  ```

  > A imagem usa Java 21, que tem suporte oficial a partir do Spark 4.

---

## 3. Passo a passo

### Passo 1: coloque os arquivos na mesma pasta

Todos os arquivos da seção 1 devem estar na mesma pasta, incluindo os dois CSVs.

### Passo 2: abra o terminal na pasta do projeto

```bash
cd caminho/para/projeto
```

### Passo 3: construa a imagem (só na primeira vez)

```bash
docker compose build
```

Repita este passo **apenas** se alterar o `Dockerfile` ou o `requirements.txt`. Alterações em `main.py`, `mapper.py` e `reducer.py` **não** exigem rebuild, porque a pasta é montada como volume no container.

### Passo 4: execute as métricas

```bash
# todas as métricas
docker compose run --rm clima-spark

# uma métrica
docker compose run --rm clima-spark temperatura

# várias métricas
docker compose run --rm clima-spark temperatura records
```

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

### Observações de interpretação

- **Temperatura média diária** = `(temp_max + temp_min) / 2`, uma aproximação usual. Ela costuma ficar um pouco acima da média real de 24 h.
- **Linha "ESPAÑA"** em `temperatura`: soma todas as estações, então regiões com mais estações pesam mais.
- **"Fez mais calor"** pode ter dois sentidos. O recorde absoluto está em `records`. A região mais quente em geral é a de maior "Media" em `temperatura`.
- A coluna **nº obs** indica quantos dias-estação entraram no cálculo. Valores baixos indicam um ano incompleto ou com falhas.
- Registros com `temp_max < temp_min` ou precipitação negativa são descartados como erro de medição.

---

## 5. Como funciona (MapReduce)

```
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
```

---

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

---

## 7. Comandos úteis

```bash
# abrir um terminal dentro do container
docker compose run --rm --entrypoint bash clima-spark

# ver o resultado bruto de uma métrica
cat salida_metricas/temperatura/part-*

# reconstruir a imagem do zero
docker compose build --no-cache
```

---

## 8. Solução de problemas

| Sintoma | Causa provável / solução |
|---------|--------------------------|
| `Cannot connect to the Docker daemon` | O Docker Desktop não está aberto. |
| `service ... has neither an image nor a build context` | Há um serviço no `docker-compose.yml` sem `build:`. Deve existir só o serviço `clima-spark`. |
| `FileNotFoundError: estaciones.csv` / `clima_espanha.csv` | O comando não foi executado na pasta do projeto, ou os CSVs não estão nela. |
| `Métricas desconocidas: ...` | Nome de métrica errado. Use `lluvia`, `dias_extremos`, `temperatura` ou `records`. |
| `JAVA_HOME is not set` / `Java gateway process exited` | Refaça a imagem com `docker compose build --no-cache`. |
| `OutOfMemoryError` ou o container encerra sem mensagem | Aumente a memória em Docker Desktop → Settings → Resources. |
| Resumo vazio para uma métrica | Ela ainda não foi executada, ou não há dados de 2023-2025 para as estações de `estaciones.csv`. |
| Alterei o código e nada mudou | Confira se o volume `.:/app` está no `docker-compose.yml`. |