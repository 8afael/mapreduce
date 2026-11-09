ATRIBUTOS = ["temp_max", "temp_min", "precipitation"]

#fabricamos un mapper que filtra y mapea los registros de las estaciones de AEMET
def crear_mapper(col, estaciones):
    def mapper(linea):
        campos = linea.split(",")
        estacion = campos[col["site"]]

        if estacion not in estaciones:
            return []
        valores = {}
        for a in ATRIBUTOS:
            try:
                valores[a] = float(campos[col[a]])
            except ValueError:
                valores[a] = None

        if (valores["temp_max"] is not None and valores["temp_min"] is not None
                and valores["temp_max"] < valores["temp_min"]):
            valores["temp_max"] = valores["temp_min"] = None

        if valores["precipitation"] is not None and valores["precipitation"] < 0:
            valores["precipitation"] = None

        return [(campos[col["date"]], estacion, estaciones[estacion], valores)]

    return mapper


def map_lluvia(r):
    p = r[3]["precipitation"]
    if p is None:
        return []

    return [((r[2], r[0][:4]), (p, 1, 1 if p >= 1.0 else 0, p))]


def map_dias_extremos(r):
    tmax, tmin = r[3]["temp_max"], r[3]["temp_min"]
    if tmax is None or tmin is None:
        return []

    return [((r[2], r[0][:4]), (1, 1 if tmax >= 35 else 0, 1 if tmin <= 0 else 0))]


def map_temperatura(r):
    tmax, tmin = r[3]["temp_max"], r[3]["temp_min"]
    if tmax is None or tmin is None:
        return []

    #temperatura media diaria aproximada como (tmax + tmin) / 2
    return [((r[2], r[0][:4]), ((tmax + tmin) / 2, tmax, tmin, 1))]


def map_records(r):
    tmax, tmin = r[3]["temp_max"], r[3]["temp_min"]
    if tmax is None or tmin is None:
        return []

    #guardamos el valor junto con la fecha y la estación donde se registró
    return [((r[2], r[0][:4]), ((tmax, r[0], r[1]), (tmin, r[0], r[1])))]