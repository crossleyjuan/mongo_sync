#!/bin/bash

mongo mongodb://localhost:27017/?replicaSet=repl1 --quiet --eval "db = db.getSiblingDB('superheroesdb'); db.superclone.drop(); db.sync.drop();"
