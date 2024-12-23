FROM streamreactor/stream-reactor-base:1.2.1

COPY target/kafka-connect-sendgrid-*.jar /opt/lenses/lib

CMD ["dumb-init", "/opt/lenses/bin/entry-point"]
