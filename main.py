import csv
import glob
import shutil
import sys

import mapper
import reducer

REDUCERS = 2
ANIOS_COMPLETOS = ("2023", "2024", "2025")
REGIONES = {"Catalunya": "Cataluña", "Comunitat Valenciana": "Comunidad Valenciana",
            "Illes Balears": "Islas Baleares", "": "Sin región"}

def tab(*campos):
    return "\t".join(str(c) for c in campos)


def leer(carpeta):
    for archivo in sorted(glob.glob(f"salida_metricas/{carpeta}/part-*")):
        with open(archivo, encoding="utf-8") as f:
            for linea in f:
                yield linea.rstrip("\n").split("\t")


def metrica_lluvia(registros):
    lluvia = (registros.flatMap(mapper.map_lluvia)
            .reduceByKey(reducer.reduce_lluvia, numPartitions=REDUCERS))


    shutil.rmtree("salida_metricas/lluvia", ignore_errors=True)
    lluvia.map(lambda kv: tab(kv[0][0], kv[0][1], f"{kv[1][0] / kv[1][1]:.3f}",
                              f"{100 * kv[1][2] / kv[1][1]:.2f}", kv[1][3], kv[1][1])
            ).saveAsTextFile("salida_metricas/lluvia")
    return lluvia


def metrica_dias_extremos(registros):
    dias_extremos = (registros.flatMap(mapper.map_dias_extremos)
                    .reduceByKey(reducer.reduce_suma, numPartitions=REDUCERS))


    shutil.rmtree("salida_metricas/dias_extremos", ignore_errors=True)
    dias_extremos.map(lambda kv: tab(kv[0][0], kv[0][1], f"{100 * kv[1][1] / kv[1][0]:.2f}",
                                     f"{100 * kv[1][2] / kv[1][0]:.2f}", kv[1][0])
                    ).saveAsTextFile("salida_metricas/dias_extremos")
    return dias_extremos


def metrica_temperatura(registros):
    temperatura = (registros.flatMap(mapper.map_temperatura)
                   .reduceByKey(reducer.reduce_suma, numPartitions=REDUCERS))


    shutil.rmtree("salida_metricas/temperatura", ignore_errors=True)
    #región, año, tmed, tmax media, tmin media, suma tmed, suma tmax, suma tmin, nº obs
    #(las sumas permiten agregar después toda España sin perder precisión)
    temperatura.map(lambda kv: tab(kv[0][0], kv[0][1],
                                   f"{kv[1][0] / kv[1][3]:.2f}", f"{kv[1][1] / kv[1][3]:.2f}",
                                   f"{kv[1][2] / kv[1][3]:.2f}", f"{kv[1][0]:.3f}",
                                   f"{kv[1][1]:.3f}", f"{kv[1][2]:.3f}", kv[1][3])
                    ).saveAsTextFile("salida_metricas/temperatura")
    return temperatura


def metrica_records(registros):
    records = (registros.flatMap(mapper.map_records)
               .reduceByKey(reducer.reduce_records, numPartitions=REDUCERS))


    shutil.rmtree("salida_metricas/records", ignore_errors=True)
    #región, año, tmax récord, fecha, estación, tmin récord, fecha, estación
    records.map(lambda kv: tab(kv[0][0], kv[0][1], *kv[1][0], *kv[1][1])
                ).saveAsTextFile("salida_metricas/records")
    return records


def resumen_lluvia():
    print("\nLluvia por región (media de los años completos 2023-2025): mm/día, % de días con lluvia")
    por_region = {}
    for region, anio, mm, pct, maximo, obs in leer("lluvia"):
        if anio in ANIOS_COMPLETOS:
            por_region.setdefault(region, []).append((float(mm), float(pct)))
    for region, vs in sorted(por_region.items(), key=lambda kv: -sum(v[0] for v in kv[1])):
        print(f"  {region:<28}{sum(v[0] for v in vs) / len(vs):6.2f} mm/día"
            f"{sum(v[1] for v in vs) / len(vs):7.1f} %")


def resumen_dias_extremos():
    print("\nDías de calor (tmax >= 35 °C) y de helada (tmin <= 0 °C) por región, 2023-2025 (% de los días):")
    por_region = {}
    for region, anio, calor, helada, obs in leer("dias_extremos"):
        if anio in ANIOS_COMPLETOS:
            por_region.setdefault(region, []).append((float(calor), float(helada)))
    for region, vs in sorted(por_region.items(), key=lambda kv: -sum(v[0] for v in kv[1])):
        print(f"  {region:<28}calor {sum(v[0] for v in vs) / len(vs):5.1f} %"
            f"   helada {sum(v[1] for v in vs) / len(vs):5.1f} %")


def resumen_temperatura():
    print("\nTemperatura media (°C) por región y año; tmax/tmin = media de las máximas/mínimas 2023-2025:")
    tabla = {}  #región -> {año: [suma tmed, suma tmax, suma tmin, obs]}
    for region, anio, _, _, _, s_med, s_max, s_min, obs in leer("temperatura"):
        if anio in ANIOS_COMPLETOS:
            tabla.setdefault(region, {})[anio] = [float(s_med), float(s_max), float(s_min), int(obs)]

    #España: suma de todas las regiones (cada día-estación pesa igual)
    espanha = {}
    for por_anio in tabla.values():
        for anio, v in por_anio.items():
            acc = espanha.setdefault(anio, [0.0, 0.0, 0.0, 0])
            for i in range(4):
                acc[i] += v[i]

    def medias(por_anio):
        tmed = {a: v[0] / v[3] for a, v in por_anio.items() if v[3]}
        total = [sum(v[i] for v in por_anio.values()) for i in range(4)]
        periodo = sum(tmed.values()) / len(tmed) if tmed else None  #media de las medias anuales
        tmax = total[1] / total[3] if total[3] else None
        tmin = total[2] / total[3] if total[3] else None
        return tmed, periodo, tmax, tmin

    def fmt(x, ancho=8):
        return f"{x:{ancho}.2f}" if x is not None else f"{'-':>{ancho}}"

    print(f"  {'Región':<28}" + "".join(f"{a:>8}" for a in ANIOS_COMPLETOS)
          + f"{'Media':>8}{'tmax':>8}{'tmin':>8}")

    def linea(nombre, por_anio):
        tmed, periodo, tmax, tmin = medias(por_anio)
        print(f"  {nombre:<28}" + "".join(fmt(tmed.get(a)) for a in ANIOS_COMPLETOS)
              + fmt(periodo) + fmt(tmax) + fmt(tmin))

    for region in sorted(tabla, key=lambda r: -(medias(tabla[r])[1] or 0)):
        linea(region, tabla[region])
    print("  " + "-" * 60)
    linea("ESPAÑA (todas las estaciones)", espanha)


def resumen_records():
    print("\nTemperaturas récord por región, 2023-2025:")
    por_region = {}  #región -> [(tmax, fecha, estación), (tmin, fecha, estación)]
    for region, anio, tmax, f_max, e_max, tmin, f_min, e_min in leer("records"):
        if anio in ANIOS_COMPLETOS:
            nuevo = [(float(tmax), f_max, e_max), (float(tmin), f_min, e_min)]
            actual = por_region.get(region)
            por_region[region] = nuevo if actual is None else [max(actual[0], nuevo[0]),
                                                                min(actual[1], nuevo[1])]
    if not por_region:
        return

    print(f"  {'Región':<28}{'máxima':>8}  {'fecha':<12}{'mínima':>8}  {'fecha':<12}")
    for region, (mx, mn) in sorted(por_region.items(), key=lambda kv: -kv[1][0][0]):
        print(f"  {region:<28}{mx[0]:8.1f}  {mx[1]:<12}{mn[0]:8.1f}  {mn[1]:<12}")

    region_cal, (mx, _) = max(por_region.items(), key=lambda kv: kv[1][0])
    region_fri, (_, mn) = min(por_region.items(), key=lambda kv: kv[1][1])
    print(f"\n  Más calor:  {region_cal}, {mx[0]:.1f} °C el {mx[1]} (estación {mx[2]})")
    print(f"  Más frío:   {region_fri}, {mn[0]:.1f} °C el {mn[1]} (estación {mn[2]})")


METRICAS = {
    "lluvia": (metrica_lluvia, resumen_lluvia),
    "dias_extremos": (metrica_dias_extremos, resumen_dias_extremos),
    "temperatura": (metrica_temperatura, resumen_temperatura),
    "records": (metrica_records, resumen_records),
}


def ejecutar_spark(elegidas):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("metricas_clima").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    #mandar los ficheros de mapper y reducer a los workers
    sc.addPyFile("mapper.py")
    sc.addPyFile("reducer.py")

    #estaciones metereológicas de AEMET -> región
    with open("estaciones.csv", encoding="utf-8") as f:
        estaciones = {r["site"]: REGIONES.get(r["region"], r["region"]) for r in csv.DictReader(f)}

    lineas = sc.textFile("clima_espanha.csv") #RDD por líneas
    cabecera = lineas.first()
    col = {nombre: i for i, nombre in enumerate(cabecera.split(","))} #indice de la cabecera

    registros = (lineas.filter(lambda l: l != cabecera) #quitamos la cabecera
                .flatMap(mapper.crear_mapper(col, estaciones)) #mapeamos los registros que estan en las estaciones de AEMET
                .cache()) #almacenamento en memoria

    for nombre in elegidas:
        METRICAS[nombre][0](registros) #ejecutamos el job MapReduce de cada métrica

    spark.stop() #ceramos la sesión y liberamos recursos


def main():
    elegidas = sys.argv[1:] or list(METRICAS) #ver metricas especificadas en la línea de comandos, o todas si no se especifica ninguna
    desconocidas = [m for m in elegidas if m not in METRICAS]
    if desconocidas:
        sys.exit(f"Métricas desconocidas: {', '.join(desconocidas)}. Disponibles: {', '.join(METRICAS)}")

    ejecutar_spark(elegidas)
    for nombre in elegidas:
        METRICAS[nombre][1]()


if __name__ == "__main__":
    main()