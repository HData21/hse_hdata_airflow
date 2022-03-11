FROM python:3.7-slim-buster


RUN apt-get -y update
RUN apt-get install -y libpq-dev vim alien autossh

ADD ./instantclient/oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm  ./
RUN alien ./oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm
RUN apt-get install libaio1
RUN dpkg -i ./oracle-instantclient19.5-basic_19.5.0.0.0-2_amd64.deb
RUN rm -rf ./oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm

# Install Airflow pre-requisites: https://airflow.apache.org/docs/apache-airflow/2.0.0/installation.html#getting-airflow
RUN apt install build-essential -y
# Install Airflow system dependencies: https://airflow.apache.org/docs/apache-airflow/stable/installation.html#system-dependencies
# NOTE: we have changed krb5-user to libkrb5-dev for a non-interactive installation
RUN apt-get install -y --no-install-recommends \
    freetds-bin \
    ldap-utils \
    libffi6 \
    libkrb5-dev \
    libsasl2-2 \
    libsasl2-modules \
    libssl1.1 \
    locales  \
    lsb-release \
    sasl2-bin \
    sqlite3 \
    unixodbc

# Install Airflow and any additonal dependencies
WORKDIR /root

COPY Makefile /root/Makefile
COPY airflow.requirements.txt /root/airflow.requirements.txt

RUN make internal-install-airflow
RUN make internal-install-deps

ADD scripts/tnsnames.ora /etc/
