# Lab 4 Kafka Streams

Цей проєкт реалізує Kafka Streams обчислення поверх `Topic1` із попередньої лабораторної роботи.

## Що обчислюється

Застосунок агрегує поїздки по даті `start_time` та записує результати назад у Kafka:

- `trip-duration-avg-by-day` - середня тривалість поїздки за день
- `trip-count-by-day` - кількість поїздок за день
- `top-start-station-by-day` - найпопулярніша початкова станція за день
- `top-3-stations-by-day` - топ-3 станцій за день з урахуванням початку і кінця поїздки

## Структура

- `src/main/java/com/example/lab4/App.java` - Kafka Streams topology
- `src/main/java/com/example/lab4/JsonSerde.java` - JSON Serde для Kafka Streams
- `docker-compose.yml` - запуск ініціалізації топіків і Java застосунку
- `scripts/create-topics.sh` - створення вихідних топіків

## Перед запуском

1. У `lab_3` потрібно запустити Kafka кластер і producer.
2. Переконайтеся, що Docker network від попередньої лабораторної існує. За замовчуванням використовується `lab_3_default`.

## Запуск

Запуск Kafka Streams застосунку:

```bash
docker compose up --build
```

Якщо мережа Docker від `lab_3` має іншу назву:

```bash
LAB3_DOCKER_NETWORK=<your_network_name> docker compose up --build
```

## Перевірка

Перевірити логи застосунку:

```bash
docker compose logs -f streams-app
```

У Kafka UI з попередньої лабораторної повинні з'явитися топіки:

- `trip-duration-avg-by-day`
- `trip-count-by-day`
- `top-start-station-by-day`
- `top-3-stations-by-day`

У кожному з них кількість повідомлень має бути більше `0` після обробки `Topic1`.

## Git

```bash
git init
git add .
git commit -m "Create Kafka Streams lab"
git branch -M main
git remote add origin <YOUR_GIT_REPO_URL>
git push -u origin main
```
