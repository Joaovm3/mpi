#!/bin/bash

# 1. Executar o ETL com Python
echo "Executando ETL em Python..."
python3 etl.py

# 2. Instalar a biblioteca libjson-c-dev com sudo
echo "Instalando libjson-c-dev..."
sudo apt install -y libjson-c-dev 

# 3. Compilar o código MPI com json-c
echo "Compilando o programa MPI..."
mpicc -o mpi mpi.c -ljson-c

# 4. Executar o programa MPI com 4 processos e medir o tempo de execução
echo "Executando o programa MPI com 4 processos..."
time mpirun -np 4 ./mpi

# 5. Navegar para o diretório parte-3
echo "Navegando para o diretório parte-3..."
cd parte-3

# 6. Iniciar o docker-compose
echo "Iniciando docker-compose..."
docker-compose up
