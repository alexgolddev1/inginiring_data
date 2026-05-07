# Шпаргалка по лабораторним роботам

Цей файл зводить в одне місце основні дії, команди та точки перевірки для всіх наявних лабораторних `md`-файлів:

- `lab_3/README.md`
- `lab_4/README.md`
- `lab_5/exercise-5-lakehousing.md`

## Загальна послідовність

1. Запустити `lab_3`, щоб підняти Kafka і наповнити `Topic1` та `Topic2`.
2. Запустити `lab_4`, щоб обробити `Topic1` через Kafka Streams.
3. Розширити стек у `lab_5`, щоб додати lakehouse на базі Iceberg + Polaris + Trino + MinIO.

---

## Lab 3: Kafka Producer

### Що робить

- Піднімає Kafka-кластер: `Zookeeper`, `Broker1`, `Broker2`, `Kafka UI`
- Python producer читає CSV і публікує записи у `Topic1` та `Topic2`

### Основні файли

- [lab_3/README.md](/home/alexgold/Lab2026/інжирінг%20даних/lab_3/README.md)
- `lab_3/docker-compose.yml`
- `lab_3/producer/`
- `lab_3/scripts/create-topics.sh`

### Запуск

```bash
cd lab_3
docker compose up --build
```

### Запуск у фоні

```bash
docker compose up -d --build
```

### Перевірка

- Kafka UI: `http://localhost:8080`
- Кластер: `lab3-cluster`
- Переконатися, що в `Topic1` і `Topic2` є повідомлення

### Корисні команди

```bash
docker compose logs -f producer
docker compose down
```

### Результат лабораторної

- Kafka-кластер працює
- CSV прочитаний
- Дані доставлені в `Topic1` і `Topic2`

---

## Lab 4: Kafka Streams

### Що робить

Kafka Streams застосунок читає `Topic1` з попередньої лабораторної та рахує агрегати по днях.

### Вихідні топіки

- `trip-duration-avg-by-day` - середня тривалість поїздки за день
- `trip-count-by-day` - кількість поїздок за день
- `top-start-station-by-day` - найпопулярніша стартова станція за день
- `top-3-stations-by-day` - топ-3 станцій за день з урахуванням старту і фінішу

### Основні файли

- [lab_4/README.md](/home/alexgold/Lab2026/інжирінг%20даних/lab_4/README.md)
- `lab_4/src/main/java/com/example/lab4/App.java`
- `lab_4/src/main/java/com/example/lab4/JsonSerde.java`
- `lab_4/docker-compose.yml`

### Перед запуском

- `lab_3` уже має бути запущена
- Docker network з `lab_3` має існувати
- За замовчуванням використовується мережа `lab_3_default`

### Запуск

```bash
cd lab_4
docker compose up --build
```

### Якщо мережа має іншу назву

```bash
LAB3_DOCKER_NETWORK=<your_network_name> docker compose up --build
```

### Перевірка

```bash
docker compose logs -f streams-app
```

У Kafka UI мають з'явитися топіки:

- `trip-duration-avg-by-day`
- `trip-count-by-day`
- `top-start-station-by-day`
- `top-3-stations-by-day`

У кожному топіку після обробки `Topic1` має бути більше `0` повідомлень.

### Результат лабораторної

- Потік з `Topic1` оброблено
- Агрегати записано в окремі Kafka topics

---

## Lab 5: Iceberg Lakehouse

### Що додається

До попередньої лабораторної додається озерне сховище даних:

- `Apache Iceberg` - формат таблиць
- `Apache Polaris` - REST-каталог Iceberg
- `Trino` - SQL engine
- `MinIO` - S3-сумісне сховище

### Логіка взаємодії

1. `Trino` звертається до `Polaris` по метадані таблиць.
2. `Polaris` зберігає метадані Iceberg у `MinIO`.
3. `Trino` читає і записує дані напряму в `MinIO`.

### Основний файл

- [lab_5/exercise-5-lakehousing.md](/home/alexgold/Lab2026/інжирінг%20даних/lab_5/exercise-5-lakehousing.md)

### Що треба додати в `docker-compose.yml`

Сервіси:

- `polaris`
- `trino`
- `minio`
- `minio-client`

Мережа:

- `local-iceberg-lakehouse`

### Конфігурація Trino

Створити каталог:

```bash
mkdir -p ./trino/catalog/
```

Створити файл `./trino/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog/
iceberg.rest-catalog.warehouse=polariscatalog
iceberg.rest-catalog.vended-credentials-enabled=true
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=root:secret
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=dummy-region
```

### Запуск стеку

```bash
docker compose up
```

### Що має бути доступно

- Trino UI: `http://localhost:8080`
- MinIO UI: `http://localhost:9001`
- Polaris API: `http://localhost:8181`

### Отримати токен Polaris

```bash
ACCESS_TOKEN=$(curl -X POST \
  http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')
```

### Створити каталог Iceberg у Polaris

```bash
curl -i -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8181/api/management/v1/catalogs \
  --json '{
    "name": "polariscatalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse",
      "s3.endpoint": "http://minio:9000",
      "s3.path-style-access": "true",
      "s3.access-key-id": "admin",
      "s3.secret-access-key": "password",
      "s3.region": "dummy-region"
    },
    "storageConfigInfo": {
      "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
      "storageType": "S3",
      "allowedLocations": [
        "s3://warehouse/*"
      ]
    }
  }'
```

### Перевірити каталог

```bash
curl -X GET http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

### Налаштувати ролі в Polaris

```bash
curl -X PUT http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"grant":{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}}'
```

```bash
curl -X POST http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"principalRole":{"name":"data_engineer"}}'
```

```bash
curl -X PUT http://localhost:8181/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"catalogRole":{"name":"catalog_admin"}}'
```

```bash
curl -X PUT http://localhost:8181/api/management/v1/principals/root/principal-roles \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"principalRole": {"name":"data_engineer"}}'
```

### Перевірити ролі root

```bash
curl -X GET http://localhost:8181/api/management/v1/principals/root/principal-roles \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

### Підключитися до Trino CLI

```bash
docker compose exec -it trino trino --server localhost:8080 --catalog iceberg
```

### SQL-шпаргалка

```sql
CREATE SCHEMA db;
USE db;

CREATE TABLE customers (
  customer_id BIGINT,
  first_name VARCHAR,
  last_name VARCHAR,
  email VARCHAR
);

INSERT INTO customers (customer_id, first_name, last_name, email)
VALUES (1, 'Rey', 'Skywalker', 'rey@rebelscum.org'),
       (2, 'Hermione', 'Granger', 'hermione@hogwarts.edu'),
       (3, 'Tony', 'Stark', 'tony@starkindustries.com');

SELECT * FROM customers;
```

### Результат лабораторної

- Піднято Iceberg lakehouse
- Trino працює через Polaris catalog
- Дані зберігаються в MinIO
- Таблиці можна створювати і читати через SQL

---

## Коротко: що перевіряти перед здачею

### Lab 3

- Kafka кластер стартує без помилок
- `Topic1` і `Topic2` створені
- producer надсилає повідомлення

### Lab 4

- Kafka Streams застосунок підключається до Kafka
- усі 4 вихідні топіки створені
- у вихідних топіках є дані

### Lab 5

- `Trino`, `Polaris`, `MinIO` доступні
- каталог `polariscatalog` створений
- роль `data_engineer` прив'язана до `root`
- через Trino створюється схема, таблиця і виконується `SELECT`
