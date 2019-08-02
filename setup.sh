#!/usr/bin/env bash

sudo apt update && sudo apt upgrade -y
sudo apt install build-essential linux-headers-$(uname -r) -y
sudo add-apt-repository ppa:openjdk-r/ppa -y
sudo apt install openjdk-8-jdk -y
sudo apt install git ant zip -y
sudo apt install python2.7 python3.7 python-pip python3-pip -y
pip3 install virtualenv
python3 -m virtualenv --version


wget http://apache.mirrors.lucidnetworks.net/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz
tar xzf apache-cassandra-3.11.4-bin.tar.gz
rm apache-cassandra-3.11.4-bin.tar.gz

wget http://download.redis.io/redis-stable.tar.gz
tar xzf redis-stable.tar.gz
rm redis-stable.tar.gz

cd redis-stable
make
sudo cp src/redis-server /usr/local/bin/
sudo cp src/redis-cli /usr/local/bin/
cd ..
