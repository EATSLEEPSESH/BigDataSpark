# BigDataSpark — Лабораторная работа №2

## Анализ больших данных — ETL на Apache Spark

## Описание работы

В рамках лабораторной работы реализован ETL-пайплайн на Apache Spark, который выполняет следующие этапы:

1. использование исходных CSV-файлов `mock_data`
2. загрузка исходных данных в PostgreSQL в таблицу `public.mock_data`
3. построение модели данных звезда в PostgreSQL с помощью Spark
4. построение 6 аналитических витрин в ClickHouse с помощью Spark

Работа выполнена в рамках обязательной части задания:

- PostgreSQL
- Apache Spark
- ClickHouse

Опциональные реализации для Cassandra, Neo4j, MongoDB и Valkey в данной работе не выполнялись.

---

## Цель работы

Цель работы — реализовать ETL-процесс с помощью Apache Spark для обработки исходных данных `mock_data(*).csv`, построения модели данных звезда в PostgreSQL и формирования аналитических витрин в ClickHouse.

---

## Что реализовано

### 1. Исходный слой данных в PostgreSQL

Исходные CSV-файлы используются для заполнения таблицы:

- `public.mock_data`

После загрузки всех 10 файлов таблица должна содержать:

- `10000` строк

### 2. Модель данных звезда в PostgreSQL

С помощью Spark-джобы `etl_to_star.py` на основе `public.mock_data` строятся таблицы измерений и таблица фактов:

- `public.dim_customer`
- `public.dim_seller`
- `public.dim_product`
- `public.dim_store`
- `public.dim_supplier`
- `public.dim_date`
- `public.fact_sales`

### 3. Витрины в ClickHouse

С помощью Spark-джобы `etl_to_clickhouse.py` на основе модели звезда PostgreSQL создаются и заполняются 6 витрин в ClickHouse:

- `lab.mart_sales_products`
- `lab.mart_sales_customers`
- `lab.mart_sales_time`
- `lab.mart_sales_stores`
- `lab.mart_sales_suppliers`
- `lab.mart_product_quality`

---

## Используемые технологии

- Docker
- Docker Compose
- PostgreSQL 15
- Apache Spark 3.5.1
- ClickHouse
- Python / PySpark
- DBeaver

---
