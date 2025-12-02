ARG TRINO_VERSION=436
FROM trinodb/trino:${TRINO_VERSION}

# Create plugin directory and copy H3 plugin
RUN mkdir -p /usr/lib/trino/plugin/h3
COPY build/libs/trino-h3-*.jar /usr/lib/trino/plugin/h3/
