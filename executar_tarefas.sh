#!/bin/bash

echo "Executando ETL em Python..."
python3 etl.py


echo "Instalando libjson-c-dev..."
sudo apt install -y libjson-c-dev 


echo "Compilando o programa MPI..."
mpicc -o mpi mpi.c -ljson-c

echo "Executando o programa MPI com 12 processos..."
time mpirun -np 12 ./mpi

echo "Navegando para o diretório parte-3..."
cd parte-3

echo "Iniciando docker-compose..."
docker-compose up
