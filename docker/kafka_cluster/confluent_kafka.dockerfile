FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      wget \
      curl \
      git \
      libssl-dev \
      libsasl2-dev \
      librdkafka-dev \
      vim \
      openssh-client \
      iputils-ping \
      net-tools \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# create app user
RUN useradd -m -s /bin/bash confluent_kafka_user
WORKDIR /home/confluent_kafka_user
USER confluent_kafka_user

ENV VENV_DIR=/home/confluent_kafka_user/venv
RUN python -m venv $VENV_DIR
ENV PATH="$VENV_DIR/bin:$PATH"
# Install Python deps as non-root
COPY --chown=confluent_kafka_user:confluent_kafka_user requirements.txt /home/confluent_kafka_user/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# copy entrypoint (owned by root then chmod)
USER root
COPY config/kafka_cluster/entrypoint2.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && chown confluent_kafka_user:confluent_kafka_user /entrypoint.sh
USER confluent_kafka_user

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
