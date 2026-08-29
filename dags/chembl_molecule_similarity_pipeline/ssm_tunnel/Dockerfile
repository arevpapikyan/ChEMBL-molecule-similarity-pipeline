FROM amazon/aws-cli:2.17.62
USER root

RUN yum install -y socat \
    && curl -sSL -o /tmp/session-manager-plugin.rpm \
        "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/linux_64bit/session-manager-plugin.rpm" \
    && yum install -y /tmp/session-manager-plugin.rpm \
    && rm -f /tmp/session-manager-plugin.rpm \
    && yum clean all

COPY tunnel-loop.sh /usr/local/bin/tunnel-loop.sh
RUN chmod +x /usr/local/bin/tunnel-loop.sh

ENTRYPOINT ["/usr/local/bin/tunnel-loop.sh"]
