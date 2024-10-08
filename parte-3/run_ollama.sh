#!/bin/bash

echo "Starting Ollama server..."
ollama serve &
#ollama pull llama3.2:1b # DESCOMENTA ESSA LINHA SE ESTIVER DANDO PAU PRA BAIXAR O OLLAMA, EU N PRECISEI PQ BAIXEI ANTES. SÓ LEMBRA DE COMENTAR DPS DE INSTALAR 1X
ollama run llama3.2:1b


echo "Waiting for Ollama server to be active..."
while [ "$(ollama list | grep 'NAME')" == "" ]; do
  sleep 1
done
