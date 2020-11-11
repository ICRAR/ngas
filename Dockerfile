# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build and cleanup
FROM alpine
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN mkdir -p /home/ngas
COPY . /home/ngas/.
RUN apk add --update bash python3-dev py3-pip alpine-sdk db-dev sqlite linux-headers
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN /home/ngas/build.sh ; /home/ngas/prepare_ngas_root.sh -f /NGAS
RUN apk del bash alpine-sdk ; cd /home/ngas ; rm -rf * .[egv]* ; find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

# Second stage build taking what's required from first stage
FROM alpine
RUN apk add --update python3 db sqlite
COPY VERSION /home/ngas/VERSION
COPY startServer.sh /home/ngas/startServer.sh
COPY --from=0 /home/ngas/. /home/ngas/.
COPY --from=0 /usr/bin/. /usr/bin/.
COPY --from=0 /usr/lib/python3.8/site-packages/. /usr/lib/python3.8/site-packages/.
COPY --from=0 /NGAS/. /NGAS/.
RUN sed -i 's/127.0.0.1/0.0.0.0/g' /NGAS/cfg/ngamsServer.conf
CMD [ "/home/ngas/startServer.sh" ]
