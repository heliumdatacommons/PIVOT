#!/bin/bash 
export LIBPROCESS_IP=$(ip route get 8.8.8.8 | head -1 | cut -d' ' -f8)
env MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so \
    java -cp $HOME/chronos*.jar org.apache.mesos.chronos.scheduler.Main $@
