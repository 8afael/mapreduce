# Imagem base do Python
FROM python:3.10-slim

# Instala o OpenJDK 21 e procps via apt
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME independente da arquitetura (arm64 no Mac M1/M2/M3, amd64 em Intel/Linux/Windows)
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Define o diretório de trabalho
WORKDIR /app

# Instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia os arquivos do projeto (o volume do compose sobrepõe /app em tempo de execução)
COPY . .

# Padrão: rodar todas as métricas
ENTRYPOINT ["python", "main.py"]
CMD []