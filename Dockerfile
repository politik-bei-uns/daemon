FROM ubuntu:16.04
LABEL maintainer "Ernesto Ruge <mail@ernestoruge.de>"
ENV PYTHONUNBUFFERED 1
ENV DEBIAN_FRONTEND noninteractive
ENV LANG en_US.utf8
ENV LC_ALL en_US.utf8
ENV LANGUAGE en_US.utf8

RUN apt-get update && \
    apt-get install -y locales apt-utils && \
    locale-gen en_US en_US.UTF-8 && \
    echo -e 'LANG="en_US.UTF-8"\nLANGUAGE="en_US:en"\n' > /etc/default/locale  && \
    apt-get dist-upgrade -y && \
    apt-get install -y apt-utils python3 python3-pip python3-dev build-essential python3-venv libboost-python-dev libbz2-dev zlib1g-dev iputils-ping curl telnet && \
    apt-get autoremove -y && \
    apt-get clean

RUN groupadd -g 1001 webdev
RUN useradd -u 1001 -g webdev -m -d /home/webdev -s /bin/bash webdev

ENV HOME /home/webdev

RUN mkdir /app
WORKDIR /app
COPY . /app

RUN rm /usr/bin/python && ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip

#RUN if [ ! -L /usr/bin/python ]; then ln -s /usr/bin/python3 /usr/bin/python; fi
#RUN if [ ! -L /usr/bin/pip ]; then ln -s /usr/bin/pip3 /usr/bin/pip; fi

USER webdev

RUN pip install -r requirements.txt