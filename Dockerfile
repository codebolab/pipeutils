# Download base image debian
FROM debian:wheezy
# Author
MAINTAINER codebolab

# Update Software repository
RUN apt-get update
RUN apt-get clean

RUN apt-get install -y python3 && \
    apt-get install -y bzip2 && \
    apt-get install -y build-essential && \ 
    apt-get install -y libssl-dev && \ 
    apt-get install -y libffi-dev && \
    apt-get install -y python-dev && \
    apt-get install -y wget
ADD . /app

RUN wget --no-check-certificate -O $HOME/miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh 

RUN /bin/bash $HOME/miniconda.sh -b -p $HOME/miniconda

WORKDIR /app

RUN /bin/bash -c "source init.sh "
