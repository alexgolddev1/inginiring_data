# Lab 3 Kafka Producer

Цей проєкт реалізує лабораторну роботу:

- Dockerized Kafka cluster із `Zookeeper`, `Broker1`, `Broker2`, `Kafka UI`
- Python producer, який читає CSV та публікує кожен запис у `Topic1` і `Topic2`

## Структура

- `docker-compose.yml` - Kafka кластер, Kafka UI, producer
- `producer/` - Python застосунок продюсера
- `scripts/create-topics.sh` - створення `Topic1` і `Topic2`
- `Divvy_Trips_2019_Q4.csv` - вхідні дані

## Запуск

```bash
docker compose up --build
```

Kafka UI буде доступний за адресою:

```text
http://localhost:8080
```

У UI потрібно відкрити кластер `lab3-cluster` і перевірити повідомлення в `Topic1` та `Topic2`.

## Корисні команди

Запуск у фоні:

```bash
docker compose up -d --build
```

Перегляд логів продюсера:

```bash
docker compose logs -f producer
```

Зупинка:

```bash
docker compose down
```

## Git

Ініціалізація репозиторію:

```bash
git init
git add .
git commit -m "Create Kafka producer lab"
```

Додавання віддаленого репозиторію та push:

```bash
git remote add origin <YOUR_GIT_REPO_URL>
git branch -M main
git push -u origin main
```

## Примітка

Schema Registry не додано, бо в цій реалізації використовується JSON-серіалізація, а не Avro.
