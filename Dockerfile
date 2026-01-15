FROM python:3.9-slim

WORKDIR /app

# Instalar dependências do sistema para Impala/Thrift
RUN apt-get update && apt-get install -y \
    build-essential \
    libsasl2-dev \
    libsasl2-modules \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expor porta padrão do Streamlit
EXPOSE 8501

# Healthcheck
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
