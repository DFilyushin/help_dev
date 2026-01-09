#!/usr/bin/env python3
"""
Скрипт безопасной отправки бэкапов в S3-совместимое объектное хранилище.
Используется для работы с Selectel-хранилищем.

Настройки в файле config.ini.
[backup]
# Директория с бэкапами
directory = /var/db_backup

# Расширения файлов для загрузки (через запятую)
extensions = .7z, .gz, .zip

# Количество дней для поиска файлов (загружаем файлы не старше N дней)
day_delta = 3

# Удалять локальные файлы после успешной загрузки
delete_after_upload = true

# Количество параллельных потоков загрузки
max_workers = 3

# Порог для multipart upload (в байтах, 100MB по умолчанию)
multipart_threshold = 104857600

# Количество повторных попыток при ошибках
max_retries = 3

[s3]
# URL эндпоинта S3
endpoint = https://storage.s3.ru-1.storage.selcloud.ru

# Имя bucket
bucket = backup

# Регион (для Selectel обычно ru-1)
region = ru-1

# Ключи доступа (ВАЖНО: защитите права доступа к этому файлу!)
access_key = 82d************************5330c
secret_key = 612************************e9dba

# Проверка SSL сертификатов (всегда true для продакшена!)
verify_ssl = false

Использование:
    python3 backup-to-s3.py [--dry-run] [--config /path/to/config.ini]
"""

import argparse
import configparser
import hashlib
import logging
import sys
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


urllib3.disable_warnings()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('/var/log/s3-backup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class BackupConfig:
    """Конфигурация бэкапа"""
    backup_dir: Path
    backup_extensions: Tuple[str, ...]
    day_delta: int
    s3_endpoint: str
    s3_bucket: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str = 'ru-1'
    verify_ssl: bool = True
    delete_after_upload: bool = True
    max_workers: int = 3
    multipart_threshold: int = 100 * 1024 * 1024  # 100 MB
    max_retries: int = 3


class S3BackupManager:
    """Менеджер загрузки бэкапов в S3"""
    
    def __init__(self, config: BackupConfig):
        self.config = config
        self.s3_client = self._create_s3_client()
        self.stats = {
            'uploaded': 0,
            'skipped': 0,
            'failed': 0,
            'deleted': 0,
            'total_bytes': 0
        }
    
    def _create_s3_client(self):
        """Создание S3 клиента с правильной конфигурацией"""
        session = boto3.Session(
            aws_access_key_id=self.config.s3_access_key,
            aws_secret_access_key=self.config.s3_secret_key
        )
        
        boto_config = Config(
            region_name=self.config.s3_region,
            retries={'max_attempts': self.config.max_retries, 'mode': 'adaptive'},
            max_pool_connections=self.config.max_workers * 2
        )
        
        return session.client(
            's3',
            endpoint_url=self.config.s3_endpoint,
            config=boto_config,
            verify=self.config.verify_ssl
        )
    
    def _calculate_file_hash(self, file_path: Path, algorithm: str = 'md5') -> str:
        """Вычисление хеша файла"""
        hash_func = hashlib.new(algorithm)
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    
    def _file_exists_in_s3(self, key: str) -> Tuple[bool, Optional[str]]:
        """
        Проверка существования файла в S3.
        Возвращает (exists, etag)
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.config.s3_bucket,
                Key=key
            )
            etag = response.get('ETag', '').strip('"')
            return True, etag
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False, None
            logger.error(f"Ошибка при проверке файла {key}: {e}")
            raise
    
    def _verify_upload(self, file_path: Path, s3_key: str) -> bool:
        """Проверка успешности загрузки через сравнение MD5"""
        try:
            local_md5 = self._calculate_file_hash(file_path, 'md5')
            
            # Получаем ETag из S3 (для простых upload это MD5)
            response = self.s3_client.head_object(
                Bucket=self.config.s3_bucket,
                Key=s3_key
            )
            s3_etag = response['ETag'].strip('"')
            
            # Для multipart upload ETag это не MD5, поэтому проверяем размер
            if '-' in s3_etag:
                local_size = file_path.stat().st_size
                s3_size = response['ContentLength']
                verified = local_size == s3_size
                if verified:
                    logger.info(f"Верификация {s3_key}: размеры совпадают ({local_size} bytes)")
                return verified
            
            # Для обычного upload сравниваем MD5
            verified = local_md5 == s3_etag
            if verified:
                logger.info(f"Верификация {s3_key}: MD5 совпадает")
            return verified
            
        except Exception as e:
            logger.error(f"Ошибка при верификации {s3_key}: {e}")
            return False
    
    def _upload_file(self, file_path: Path, dry_run: bool = False) -> bool:
        """
        Загрузка файла в S3 с проверкой.
        Возвращает True при успехе.
        """
        s3_key = file_path.name
        file_size = file_path.stat().st_size
        
        try:
            # Проверка существования
            exists, s3_etag = self._file_exists_in_s3(s3_key)
            if exists:
                logger.info(f"Пропущен {s3_key}: уже существует в S3")
                self.stats['skipped'] += 1
                return False
            
            if dry_run:
                logger.info(f"[DRY-RUN] Будет загружен: {s3_key} ({file_size / 1024 / 1024:.2f} MB)")
                return False
            
            # Загрузка файла
            logger.info(f"Загрузка {s3_key} ({file_size / 1024 / 1024:.2f} MB)...")
            
            extra_args = {
                'StorageClass': 'STANDARD',
                'Metadata': {
                    'original-path': str(file_path),
                    'upload-date': datetime.now().isoformat()
                }
            }
            
            # Для больших файлов используем multipart upload автоматически
            self.s3_client.upload_file(
                str(file_path),
                self.config.s3_bucket,
                s3_key,
                ExtraArgs=extra_args,
                Config=boto3.s3.transfer.TransferConfig(
                    multipart_threshold=self.config.multipart_threshold,
                    max_concurrency=10,
                    use_threads=True
                )
            )
            
            # Верификация загрузки
            if not self._verify_upload(file_path, s3_key):
                logger.error(f"Верификация не прошла для {s3_key}")
                # Удаляем битый файл из S3
                self.s3_client.delete_object(Bucket=self.config.s3_bucket, Key=s3_key)
                return False
            
            logger.info(f"✓ Успешно загружен: {s3_key}")
            self.stats['uploaded'] += 1
            self.stats['total_bytes'] += file_size
            
            # Удаление локального файла после успешной загрузки
            if self.config.delete_after_upload:
                file_path.unlink()
                self.stats['deleted'] += 1
                logger.info(f"Удален локальный файл: {file_path}")
            
            return True
            
        except ClientError as e:
            logger.error(f"S3 ошибка при загрузке {s3_key}: {e}")
            self.stats['failed'] += 1
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке {s3_key}: {e}")
            self.stats['failed'] += 1
            return False
    
    def get_files_for_backup(self) -> List[Path]:
        """Получение списка файлов для загрузки"""
        if not self.config.backup_dir.exists():
            logger.error(f"Директория не существует: {self.config.backup_dir}")
            return []
        
        current_date = datetime.now()
        cutoff_date = current_date - timedelta(days=self.config.day_delta)
        
        files_for_backup = []
        for file_path in self.config.backup_dir.iterdir():
            if file_path.is_file() and file_path.suffix in self.config.backup_extensions:
                # Проверка даты создания файла
                create_date = datetime.fromtimestamp(file_path.stat().st_ctime)
                if create_date >= cutoff_date:
                    files_for_backup.append(file_path)
        
        return sorted(files_for_backup, key=lambda f: f.stat().st_ctime)
    
    def run_backup(self, dry_run: bool = False) -> bool:
        """Запуск процесса бэкапа"""
        logger.info("=" * 80)
        logger.info("Запуск процесса загрузки бэкапов в S3")
        if dry_run:
            logger.info("РЕЖИМ DRY-RUN: файлы не будут загружены")
        logger.info(f"Хранилище: {self.config.s3_endpoint}")
        logger.info(f"Bucket: {self.config.s3_bucket}")
        logger.info("=" * 80)
        
        # Получение списка файлов
        files = self.get_files_for_backup()
        if not files:
            logger.info("Нет файлов для загрузки")
            return True
        
        logger.info(f"Найдено файлов для загрузки: {len(files)}")
        total_size = sum(f.stat().st_size for f in files)
        logger.info(f"Общий размер: {total_size / 1024 / 1024:.2f} MB")
        
        # Параллельная загрузка файлов
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self._upload_file, file_path, dry_run): file_path
                for file_path in files
            }
            
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Критическая ошибка при обработке {file_path}: {e}")
        
        # Вывод статистики
        logger.info("=" * 80)
        logger.info("Статистика выполнения:")
        logger.info(f"  Загружено:     {self.stats['uploaded']}")
        logger.info(f"  Пропущено:     {self.stats['skipped']}")
        logger.info(f"  Ошибок:        {self.stats['failed']}")
        logger.info(f"  Удалено:       {self.stats['deleted']}")
        logger.info(f"  Передано:      {self.stats['total_bytes'] / 1024 / 1024:.2f} MB")
        logger.info("=" * 80)
        
        return self.stats['failed'] == 0


def load_config(config_path: Path) -> BackupConfig:
    """Загрузка конфигурации из INI файла"""
    if not config_path.exists():
        raise FileNotFoundError(f"Конфиг файл не найден: {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    s3_section = config['s3']
    backup_section = config['backup']
    
    return BackupConfig(
        backup_dir=Path(backup_section['directory']),
        backup_extensions=tuple(ext.strip() for ext in backup_section['extensions'].split(',')),
        day_delta=backup_section.getint('day_delta', 3),
        s3_endpoint=s3_section['endpoint'],
        s3_bucket=s3_section['bucket'],
        s3_access_key=s3_section['access_key'],
        s3_secret_key=s3_section['secret_key'],
        s3_region=s3_section.get('region', 'ru-1'),
        verify_ssl=s3_section.getboolean('verify_ssl', True),
        delete_after_upload=backup_section.getboolean('delete_after_upload', True),
        max_workers=backup_section.getint('max_workers', 3),
        multipart_threshold=backup_section.getint('multipart_threshold', 100 * 1024 * 1024),
        max_retries=backup_section.getint('max_retries', 3)
    )


def main():
    parser = argparse.ArgumentParser(
        description='Загрузка бэкапов в S3-совместимое хранилище'
    )
    parser.add_argument(
        '--config',
        type=Path,
        default=Path('/etc/backup/s3-config.ini'),
        help='Путь к конфигурационному файлу'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Режим симуляции без реальной загрузки'
    )
    
    args = parser.parse_args()
    
    try:
        # Загрузка конфигурации
        config = load_config(args.config)
        
        # Запуск бэкапа
        manager = S3BackupManager(config)
        success = manager.run_backup(dry_run=args.dry_run)
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()