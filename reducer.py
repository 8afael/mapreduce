def reduce_suma(a, b):
    return tuple(x + y for x, y in zip(a, b))


def reduce_lluvia(a, b):
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2], max(a[3], b[3]))


def reduce_records(a, b):
    #máximo de las máximas y mínimo de las mínimas, conservando fecha y estación
    #(comparar tuplas desempata por fecha, así el resultado no depende del orden)
    return (max(a[0], b[0]), min(a[1], b[1]))