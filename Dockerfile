ARG TRINO_VERSION=479
FROM trinodb/trino:${TRINO_VERSION}

# Force immediate symbol resolution for native libraries
# This prevents SIGSEGV during lazy loading of JNI symbols
ENV LD_BIND_NOW=1

# Create plugin directory and copy H3 plugin
RUN mkdir -p /usr/lib/trino/plugin/h3
COPY build/libs/trino-h3-*.jar /usr/lib/trino/plugin/h3/