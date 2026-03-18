FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    supervisor \
    curl \
    gnupg \
    python3.11 \
    python3-pip \
    python3.11-venv \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsS https://www.pgadmin.org/static/packages_pgadmin_org.pub | \
    gpg --dearmor -o /usr/share/keyrings/packages-pgadmin-org.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/packages-pgadmin-org.gpg] https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/bookworm pgadmin4 main" \
    > /etc/apt/sources.list.d/pgadmin4.list \
    && apt-get update && apt-get install -y pgadmin4-web \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN /usr/pgadmin4/bin/setup-web.sh --yes || true

RUN mkdir -p /var/log/supervisor

RUN python3.11 -m venv /venv
ENV PATH="/venv/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app .

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy pgAdmin config to override bind address
COPY pgadmin_config.py /usr/pgadmin4/web/config_local.py

EXPOSE 8000 5050

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
