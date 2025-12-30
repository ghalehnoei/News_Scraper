# راهنمای پیکربندی PostgreSQL در Docker

این راهنما نحوه پیکربندی و استفاده از PostgreSQL در محیط Docker را شرح می‌دهد.

## فهرست مطالب

1. [نصب و راه‌اندازی اولیه](#نصب-و-راه-اندازی-اولیه)
2. [پیکربندی متغیرهای محیطی](#پیکربندی-متغیرهای-محیطی)
3. [مدیریت Volume و داده‌ها](#مدیریت-volume-و-داده-ها)
4. [پیکربندی شبکه](#پیکربندی-شبکه)
5. [اتصال به دیتابیس](#اتصال-به-دیتابیس)
6. [مدیریت و نگهداری](#مدیریت-و-نگهداری)
7. [امنیت](#امنیت)
8. [عیب‌یابی](#عیب-یابی)

---

## نصب و راه‌اندازی اولیه

### 1. اجرای ساده PostgreSQL

```bash
docker run --name postgres-container \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -d postgres:15-alpine
```

### 2. اجرا با Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    container_name: postgres-db
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: news_db
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

### 3. اجرای سرویس

```bash
docker-compose up -d postgres
```

---

## پیکربندی متغیرهای محیطی

### متغیرهای اصلی

| متغیر | توضیح | مقدار پیش‌فرض |
|------|-------|---------------|
| `POSTGRES_USER` | نام کاربری ادمین | `postgres` |
| `POSTGRES_PASSWORD` | رمز عبور ادمین | (اجباری) |
| `POSTGRES_DB` | نام دیتابیس اولیه | `POSTGRES_USER` |
| `POSTGRES_INITDB_ARGS` | آرگومان‌های initdb | - |
| `POSTGRES_HOST_AUTH_METHOD` | روش احراز هویت | `md5` |

### مثال پیکربندی پیشرفته

```yaml
services:
  postgres:
    image: postgres:15-alpine
    environment:
      # احراز هویت
      POSTGRES_USER: ${POSTGRES_USER:-postgres}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-postgres}
      POSTGRES_DB: ${POSTGRES_DB:-news_db}
      
      # تنظیمات initdb
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=C"
      
      # تنظیمات امنیتی
      POSTGRES_HOST_AUTH_METHOD: md5
      
      # متغیرهای سفارشی PostgreSQL
      POSTGRES_MULTIPLE_DATABASES: "db1,db2,db3"
```

### استفاده از فایل .env

ایجاد فایل `.env` در کنار `docker-compose.yml`:

```env
# PostgreSQL Configuration
POSTGRES_USER=admin
POSTGRES_PASSWORD=secure_password_123
POSTGRES_DB=news_db

# Network Configuration
POSTGRES_HOST=10.20.10.10
POSTGRES_PORT=5432
```

---

## مدیریت Volume و داده‌ها

### 1. Volume نامگذاری شده (توصیه می‌شود)

```yaml
services:
  postgres:
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
    driver: local
```

### 2. Volume با مسیر مشخص

```yaml
services:
  postgres:
    volumes:
      - ./postgres-data:/var/lib/postgresql/data
```

### 3. Backup و Restore

#### Backup

```bash
# Backup یک دیتابیس
docker exec postgres-container pg_dump -U postgres news_db > backup.sql

# Backup تمام دیتابیس‌ها
docker exec postgres-container pg_dumpall -U postgres > backup_all.sql

# Backup با فشرده‌سازی
docker exec postgres-container pg_dump -U postgres news_db | gzip > backup.sql.gz
```

#### Restore

```bash
# Restore از فایل
cat backup.sql | docker exec -i postgres-container psql -U postgres -d news_db

# Restore از فایل فشرده
gunzip < backup.sql.gz | docker exec -i postgres-container psql -U postgres -d news_db
```

### 4. پاک کردن داده‌ها

```bash
# توقف و حذف container
docker-compose down

# حذف container و volume
docker-compose down -v

# حذف فقط volume
docker volume rm news-scraping_postgres_data
```

---

## پیکربندی شبکه

### 1. شبکه پیش‌فرض Docker

```yaml
services:
  postgres:
    ports:
      - "5432:5432"  # host:container
```

### 2. شبکه اختصاصی با IP ثابت

```yaml
services:
  postgres:
    networks:
      news_network:
        ipv4_address: 10.20.10.10

networks:
  news_network:
    driver: bridge
    ipam:
      config:
        - subnet: 10.20.10.0/24
          gateway: 10.20.10.1
```

### 3. اتصال سرویس‌ها به PostgreSQL

```yaml
services:
  api:
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql+asyncpg://postgres:postgres@10.20.10.10:5432/news_db
```

---

## اتصال به دیتابیس

### 1. از داخل Container

```bash
# اتصال به PostgreSQL
docker exec -it postgres-container psql -U postgres -d news_db

# یا
docker-compose exec postgres psql -U postgres -d news_db
```

### 2. از Host (با Port Mapping)

```bash
# با psql (اگر نصب شده باشد)
psql -h localhost -p 5432 -U postgres -d news_db

# با Connection String
psql "postgresql://postgres:postgres@localhost:5432/news_db"
```

### 3. از Application (Python)

```python
# با asyncpg
import asyncpg

async def connect():
    conn = await asyncpg.connect(
        host='10.20.10.10',
        port=5432,
        user='postgres',
        password='postgres',
        database='news_db'
    )
    return conn

# با SQLAlchemy
from sqlalchemy import create_engine

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@10.20.10.10:5432/news_db"
engine = create_engine(DATABASE_URL)
```

### 4. Connection String Format

```
postgresql://[user]:[password]@[host]:[port]/[database]
postgresql+asyncpg://[user]:[password]@[host]:[port]/[database]  # برای asyncpg
```

---

## مدیریت و نگهداری

### 1. Health Check

```yaml
services:
  postgres:
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
```

### 2. لاگ‌ها

```bash
# مشاهده لاگ‌ها
docker logs postgres-container

# دنبال کردن لاگ‌ها
docker logs -f postgres-container

# لاگ‌های اخیر
docker logs --tail 100 postgres-container
```

### 3. بررسی وضعیت

```bash
# وضعیت container
docker ps | grep postgres

# استفاده از منابع
docker stats postgres-container

# بررسی health check
docker inspect postgres-container | grep Health -A 10
```

### 4. به‌روزرسانی

```bash
# توقف container
docker-compose stop postgres

# تغییر image در docker-compose.yml
# postgres:15-alpine -> postgres:16-alpine

# اجرای مجدد
docker-compose up -d postgres
```

---

## امنیت

### 1. رمز عبور قوی

```yaml
environment:
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}  # از .env یا secret manager
```

### 2. محدود کردن دسترسی

```yaml
services:
  postgres:
    # فقط در شبکه داخلی
    ports: []  # حذف port mapping
    
    # یا محدود کردن به IP خاص
    ports:
      - "127.0.0.1:5432:5432"
```

### 3. استفاده از Secrets

```yaml
services:
  postgres:
    secrets:
      - postgres_password
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password

secrets:
  postgres_password:
    external: true
```

### 4. تنظیمات امنیتی PostgreSQL

```yaml
environment:
  POSTGRES_HOST_AUTH_METHOD: md5  # یا scram-sha-256
```

### 5. Firewall Rules

```bash
# فقط اجازه دسترسی از شبکه داخلی
ufw allow from 10.20.10.0/24 to any port 5432
```

---

## عیب‌یابی

### 1. Container اجرا نمی‌شود

```bash
# بررسی لاگ‌ها
docker logs postgres-container

# بررسی وضعیت
docker ps -a | grep postgres

# اجرای دستی برای دیباگ
docker run -it --rm postgres:15-alpine sh
```

### 2. مشکل اتصال

```bash
# بررسی اینکه PostgreSQL در حال اجرا است
docker exec postgres-container pg_isready -U postgres

# بررسی شبکه
docker network inspect news_network

# تست اتصال از container دیگر
docker exec -it api-container ping 10.20.10.10
```

### 3. مشکل Volume

```bash
# بررسی volume
docker volume ls
docker volume inspect news-scraping_postgres_data

# بررسی مجوزها
docker exec postgres-container ls -la /var/lib/postgresql/data
```

### 4. مشکل Performance

```bash
# بررسی استفاده از منابع
docker stats postgres-container

# بررسی query‌های کند
docker exec postgres-container psql -U postgres -c "
  SELECT pid, now() - pg_stat_activity.query_start AS duration, query 
  FROM pg_stat_activity 
  WHERE state = 'active' AND now() - pg_stat_activity.query_start > interval '5 seconds';
"
```

### 5. Reset دیتابیس

```bash
# توقف container
docker-compose stop postgres

# حذف volume
docker volume rm news-scraping_postgres_data

# اجرای مجدد
docker-compose up -d postgres
```

---

## مثال کامل Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    container_name: news-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-postgres}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-postgres}
      POSTGRES_DB: ${POSTGRES_DB:-news_db}
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=C"
      POSTGRES_HOST_AUTH_METHOD: md5
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-scripts:/docker-entrypoint-initdb.d  # اسکریپت‌های اولیه
    ports:
      - "${POSTGRES_PORT:-5432}:5432"
    networks:
      news_network:
        ipv4_address: 10.20.10.10
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-postgres}"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
    restart: unless-stopped
    command:
      - "postgres"
      - "-c"
      - "max_connections=200"
      - "-c"
      - "shared_buffers=256MB"

volumes:
  postgres_data:
    driver: local

networks:
  news_network:
    driver: bridge
    ipam:
      config:
        - subnet: 10.20.10.0/24
          gateway: 10.20.10.1
```

---

## دستورات مفید

```bash
# اجرای query
docker exec postgres-container psql -U postgres -d news_db -c "SELECT version();"

# لیست دیتابیس‌ها
docker exec postgres-container psql -U postgres -l

# لیست جداول
docker exec postgres-container psql -U postgres -d news_db -c "\dt"

# مشاهده کاربران
docker exec postgres-container psql -U postgres -c "\du"

# Restart PostgreSQL
docker-compose restart postgres

# مشاهده تنظیمات
docker exec postgres-container psql -U postgres -c "SHOW ALL;"
```

---

## منابع بیشتر

- [PostgreSQL Docker Hub](https://hub.docker.com/_/postgres)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

---

**نکته مهم**: همیشه از رمز عبور قوی استفاده کنید و آن را در فایل `.env` یا سیستم مدیریت secrets نگهداری کنید. هرگز رمز عبور را مستقیماً در `docker-compose.yml` قرار ندهید.

