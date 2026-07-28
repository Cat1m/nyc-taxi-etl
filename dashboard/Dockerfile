# Metabase build tùy chỉnh: base Debian (eclipse-temurin), không dùng image
# metabase/metabase chính thức (Alpine) vì driver DuckDB cộng đồng
# (motherduckdb/metabase_duckdb_driver) không tương thích glibc của Alpine.
# Tự tải metabase.jar + driver jar theo đúng version pin, thay vì cài qua image
# có sẵn -- Metabase chưa có driver DuckDB chính thức.
FROM eclipse-temurin:21-jre-jammy

ARG METABASE_VERSION=0.58.9
ARG METABASE_DUCKDB_DRIVER_VERSION=1.4.3.1

ENV MB_PLUGINS_DIR=/home/metabase/plugins/

RUN groupadd -r metabase && useradd -r -g metabase metabase

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# /home/metabase/plugins chứa driver DuckDB; /home/metabase/data là nơi mount
# warehouse.duckdb từ host (xem docker-compose.yaml).
RUN mkdir -p /home/metabase/plugins /home/metabase/data && \
    chown -R metabase:metabase /home/metabase

WORKDIR /home/metabase
ADD --chown=metabase:metabase https://downloads.metabase.com/v${METABASE_VERSION}/metabase.jar /home/metabase/
ADD --chown=metabase:metabase https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/${METABASE_DUCKDB_DRIVER_VERSION}/duckdb.metabase-driver.jar /home/metabase/plugins/

RUN chmod 755 /home/metabase/metabase.jar && \
    chmod 755 /home/metabase/plugins/duckdb.metabase-driver.jar

EXPOSE 3000

USER metabase

CMD ["java", "-jar", "/home/metabase/metabase.jar"]
