FROM python:3.8-slim-buster

WORKDIR /app

RUN pip install Flask requests pandas

COPY . .

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0", "--debug"]

