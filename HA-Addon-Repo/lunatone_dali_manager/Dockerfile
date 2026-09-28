ARG BUILD_FROM
FROM ${BUILD_FROM}

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

COPY app /app
COPY www /www
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
