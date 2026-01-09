#!/bin/bash

# ============================================================
# Скрипт резервного копирования PostgreSQL с архивацией 7z
# Пароль доступа для postgresql хранится в ~/.pgpass
# localhost:5432:db_name:postgres:password
# chmod 0600 ~/.pgpass
# ============================================================

set -euo pipefail  # Прерывать выполнение при ошибках

# --- Конфигурация ---
PATH=/etc:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin
BACKUP_DIR=/var/db_backup
DB_USER=postgres
DB_NAME=db_name
DB_HOST=localhost
DB_PORT=5432
DATE_FORMAT=$(date "+%Y-%m-%d_%H-%M")
LOG_FILE="${BACKUP_DIR}/backup.log"
LOCK_FILE="${BACKUP_DIR}/backup.lock"

# Настройки ротации
KEEP_DAILY_DAYS=61      # Хранить ежедневные бэкапы
KEEP_MONTHLY_DAYS=365   # Хранить месячные бэкапы (15-е число)

# Настройки 7z
ZIP_PASSWORD="your_secure_password_here"  # ВАЖНО: Замените на безопасный пароль
COMPRESSION_LEVEL=9      # 0-9, где 9 - максимальное сжатие

# --- Функции ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

error_exit() {
    log "ERROR: $1"
    cleanup
    exit 1
}

cleanup() {
    rm -f "$LOCK_FILE"
}

check_dependencies() {
    local deps=(pg_dump 7z find)
    for cmd in "${deps[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            error_exit "Требуемая утилита не найдена: $cmd"
        fi
    done
}

# --- Основной код ---
trap cleanup EXIT INT TERM

log "=== Начало резервного копирования ==="

# Проверка блокировки (предотвращение параллельного запуска)
if [ -f "$LOCK_FILE" ]; then
    error_exit "Скрипт уже выполняется (найден файл блокировки: $LOCK_FILE)"
fi
touch "$LOCK_FILE"

# Проверка зависимостей
check_dependencies

# Создание директории для бэкапов
mkdir -p "$BACKUP_DIR" || error_exit "Не удалось создать директорию $BACKUP_DIR"

# Имена файлов
DUMP_FILE="${BACKUP_DIR}/${DB_NAME}_${DATE_FORMAT}.backup"
ARCHIVE_FILE="${BACKUP_DIR}/${DB_NAME}_${DATE_FORMAT}.7z"

# Проверка наличия .pgpass для безопасности
if [ ! -f "$HOME/.pgpass" ]; then
    log "WARNING: Файл .pgpass не найден. Рекомендуется использовать .pgpass вместо PGPASSWORD"
fi

# Создание дампа базы данных
log "Создание дампа базы данных $DB_NAME..."
if pg_dump \
    -U "$DB_USER" \
    -p "$DB_PORT" \
    -h "$DB_HOST" \
    -d "$DB_NAME" \
    --encoding=UTF8 \
    -Fc \
    --no-owner \
    --no-acl \
    -f "$DUMP_FILE" 2>> "$LOG_FILE"; then
    
    DUMP_SIZE=$(du -h "$DUMP_FILE" | cut -f1)
    log "Дамп успешно создан: $DUMP_FILE ($DUMP_SIZE)"
else
    error_exit "Ошибка при создании дампа базы данных"
fi

# Архивация дампа с помощью 7z
log "Архивация дампа в 7z..."
if 7z a \
    -t7z \
    -m0=lzma2 \
    -mx="$COMPRESSION_LEVEL" \
    -mfb=64 \
    -md=32m \
    -ms=on \
    -mhe=on \
    -p"$ZIP_PASSWORD" \
    "$ARCHIVE_FILE" \
    "$DUMP_FILE" > /dev/null 2>> "$LOG_FILE"; then
    
    ARCHIVE_SIZE=$(du -h "$ARCHIVE_FILE" | cut -f1)
    
    # Вычисление процента сжатия (без bc)
    DUMP_BYTES=$(stat -c%s "$DUMP_FILE" 2>/dev/null || stat -f%z "$DUMP_FILE")
    ARCHIVE_BYTES=$(stat -c%s "$ARCHIVE_FILE" 2>/dev/null || stat -f%z "$ARCHIVE_FILE")
    COMPRESSION_RATIO=$((ARCHIVE_BYTES * 100 / DUMP_BYTES))
    
    log "Архив успешно создан: $ARCHIVE_FILE ($ARCHIVE_SIZE, сжатие до ${COMPRESSION_RATIO}%)"
    
    # Удаление исходного дампа после успешной архивации
    rm -f "$DUMP_FILE"
    log "Исходный дамп удален"
else
    error_exit "Ошибка при создании архива"
fi

# Проверка целостности архива
log "Проверка целостности архива..."
if 7z t -p"$ZIP_PASSWORD" "$ARCHIVE_FILE" > /dev/null 2>> "$LOG_FILE"; then
    log "Архив прошел проверку целостности"
else
    error_exit "Архив поврежден!"
fi

# Ротация старых бэкапов
log "Удаление старых бэкапов..."

# Удаляем ежедневные бэкапы старше KEEP_DAILY_DAYS дней (кроме 15-го числа)
DELETED_COUNT=$(find "$BACKUP_DIR" \
    -type f \
    -name "${DB_NAME}_*-[0-9][0-9]_*.7z" \
    \( -name "*-1[^5]_*" -o -name "*-[023]?_*" \) \
    -mtime +"$KEEP_DAILY_DAYS" \
    -delete -print | wc -l)
log "Удалено ежедневных бэкапов: $DELETED_COUNT"

# Удаляем месячные бэкапы (15-е число) старше KEEP_MONTHLY_DAYS дней
DELETED_MONTHLY=$(find "$BACKUP_DIR" \
    -type f \
    -name "${DB_NAME}_*-15_*.7z" \
    -mtime +"$KEEP_MONTHLY_DAYS" \
    -delete -print | wc -l)
log "Удалено месячных бэкапов: $DELETED_MONTHLY"

# Статистика по свободному месту
DISK_USAGE=$(df -h "$BACKUP_DIR" | tail -1 | awk '{print "Использовано: "$3" из "$2" ("$5")"}')
log "Дисковое пространство: $DISK_USAGE"

# Опционально: копирование на удаленный сервер
# Раскомментируйте и настройте при необходимости
# log "Копирование на удаленный сервер..."
# if scp -i /path/to/key "$ARCHIVE_FILE" user@remote_host:/path/to/backup/; then
#     log "Файл успешно скопирован на удаленный сервер"
# else
#     log "WARNING: Не удалось скопировать файл на удаленный сервер"
# fi

log "=== Резервное копирование завершено успешно ==="
exit 0