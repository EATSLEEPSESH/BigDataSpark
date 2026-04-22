# BigDataSpark — Лабораторная работа №2

## Тема
ETL-пайплайн на Apache Spark:
1. загрузка исходных CSV в PostgreSQL
2. построение модели данных звезда в PostgreSQL
3. построение 6 витрин в ClickHouse

## Используемые технологии
- Docker / Docker Compose
- PostgreSQL
- Apache Spark
- ClickHouse
- DBeaver

## Структура решения

### Источник данных
Используются 10 файлов `mock_data(*).csv`, по 1000 строк каждый.  
После загрузки в PostgreSQL таблица `public.mock_data` содержит 10000 строк.

### Этап 1. Raw слой в PostgreSQL
Исходные CSV загружаются в таблицу:

- `public.mock_data`

### Этап 2. Модель звезда в PostgreSQL
С помощью Spark реализована трансформация `raw -> star`, в результате которой создаются таблицы:

- `public.dim_customer`
- `public.dim_seller`
- `public.dim_product`
- `public.dim_store`
- `public.dim_supplier`
- `public.dim_date`
- `public.fact_sales`

### Этап 3. Витрины в ClickHouse
С помощью Spark реализована трансформация `star -> marts`, в результате которой создаются и заполняются 6 витрин:

- `lab.mart_sales_products`
- `lab.mart_sales_customers`
- `lab.mart_sales_time`
- `lab.mart_sales_stores`
- `lab.mart_sales_suppliers`
- `lab.mart_product_quality`

## Назначение витрин

### 1. Витрина продаж по продуктам
Используется для:
- анализа выручки по продуктам
- анализа количества продаж
- анализа рейтинга и отзывов

### 2. Витрина продаж по клиентам
Используется для:
- анализа покупателей
- анализа сумм покупок
- анализа среднего чека

### 3. Витрина продаж по времени
Используется для:
- анализа трендов продаж по месяцам
- анализа сезонности
- анализа среднего размера заказа

### 4. Витрина продаж по магазинам
Используется для:
- анализа эффективности магазинов
- сравнения выручки по магазинам
- анализа среднего чека по магазинам

### 5. Витрина продаж по поставщикам
Используется для:
- анализа эффективности поставщиков
- сравнения выручки по поставщикам
- анализа средней цены товаров поставщика

### 6. Витрина качества продукции
Используется для:
- анализа рейтингов товаров
- анализа отзывов
- сопоставления рейтинга и объёма продаж

## Состав репозитория

- `docker-compose.yml` — контейнеры PostgreSQL, Spark, Spark Worker, ClickHouse
- `app/etl_to_star.py` — Spark ETL из `mock_data` в модель звезда PostgreSQL
- `app/etl_to_clickhouse.py` — Spark ETL из модели звезда PostgreSQL в витрины ClickHouse
- `app/clickhouse_marts.sql` — создание базы и 6 витрин в ClickHouse
- `mock_data(1).csv ... mock_data(10).csv` — исходные данные

## Как запустить проект

### 1. Поднять контейнеры

```bash
docker compose up -d
