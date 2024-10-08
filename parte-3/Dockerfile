# Usando uma imagem Python 3.9 slim
FROM python:3.8-slim-buster

# Definir diretório de trabalho
WORKDIR /app

# Instalar Flask diretamente
RUN pip install Flask requests pandas

# Copiar o código do app
COPY . .

# Expôr a porta 5000 para o Flask
EXPOSE 5000

# Comando para rodar a aplicação Flask
CMD ["flask", "run", "--host=0.0.0.0", "--debug"]

