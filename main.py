import sqlite3
import pandas as pd
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path: str = "marine_life.db"):
        """Инициализация подключения к базе данных"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()

    def create_tables(self):
        """Создание всех таблиц в базе данных"""
        cursor = self.conn.cursor()

        tables = [
            """CREATE TABLE IF NOT EXISTS Organism (
                organism_id TEXT PRIMARY KEY,
                scientific_name TEXT NOT NULL,
                vernacular_name TEXT,
                taxon_rank TEXT,
                organism_name TEXT,
                sex TEXT,
                organism_remarks TEXT
            )""",

            """CREATE TABLE IF NOT EXISTS Observer (
                observer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_by TEXT NOT NULL UNIQUE,
                institution_code TEXT
            )""",

            """CREATE TABLE IF NOT EXISTS Location (
                location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                higher_geography TEXT,
                water_body TEXT,
                locality TEXT,
                verbatim_locality TEXT,
                UNIQUE(higher_geography, water_body, locality, verbatim_locality)
            )""",

            """CREATE TABLE IF NOT EXISTS Event (
                event_id TEXT PRIMARY KEY,
                basis_of_record TEXT,
                individual_count INTEGER,
                preparations TEXT,
                occurrence_remarks TEXT
            )""",

            """CREATE TABLE IF NOT EXISTS Media (
                media_id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_resource TEXT,
                external_resource_thumb TEXT,
                license TEXT,
                rights_holder TEXT,
                catalog_number TEXT,
                UNIQUE(external_resource, catalog_number)
            )""",

            """CREATE TABLE IF NOT EXISTS DatasetMetadata (
                dataset_metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
                oid TEXT,
                type TEXT,
                modified TEXT,
                language TEXT
            )""",

            """CREATE TABLE IF NOT EXISTS Record (
                occurrence_id TEXT PRIMARY KEY,
                decimal_latitude REAL,
                decimal_longitude REAL,
                event_date TEXT,
                event_time TEXT,
                coordinate_precision REAL,
                geodetic_datum TEXT,
                event_id TEXT,
                organism_id TEXT,
                observer_id INTEGER,
                location_id INTEGER,
                media_id INTEGER,
                dataset_metadata_id INTEGER,
                FOREIGN KEY (event_id) REFERENCES Event(event_id),
                FOREIGN KEY (organism_id) REFERENCES Organism(organism_id),
                FOREIGN KEY (observer_id) REFERENCES Observer(observer_id),
                FOREIGN KEY (location_id) REFERENCES Location(location_id),
                FOREIGN KEY (media_id) REFERENCES Media(media_id),
                FOREIGN KEY (dataset_metadata_id) REFERENCES DatasetMetadata(dataset_metadata_id)
            )"""
        ]

        for table_sql in tables:
            cursor.execute(table_sql)

        self.conn.commit()
        logger.info("Таблицы созданы успешно")

    def load_csv_data(self, csv_file_path: str):
        """Загрузка данных из CSV файла"""
        try:
            df = pd.read_csv(csv_file_path)
            logger.info(f"Загружено {len(df)} записей из CSV файла")

            for index, row in df.iterrows():
                try:
                    self._process_csv_row(row)
                except Exception as e:
                    logger.error(f"Ошибка обработки строки {index}: {e}")
                    continue

            self.conn.commit()
            logger.info("Данные успешно загружены в базу данных")

        except Exception as e:
            logger.error(f"Ошибка загрузки CSV файла: {e}")
            self.conn.rollback()

    def _process_csv_row(self, row):
        """Обработка одной строки CSV и добавление в БД"""
        # Парсим JSON данные если они есть
        notes_data = {}
        if pd.notna(row.get('notes')):
            try:
                notes_data = json.loads(row['notes'])
            except:
                pass

        # 1. Добавляем млекопитающего
        organism_id = self._insert_organism(row)

        # 2. Добавляем наблюдателя
        observer_id = self._insert_observer(row)

        # 3. Добавляем локацию
        location_id = self._insert_location(row)

        # 4. Добавляем событие
        event_id = self._insert_event(row)

        # 5. Добавляем медиа
        media_id = self._insert_media(row)

        # 6. Добавляем метаданные датасета
        dataset_metadata_id = self._insert_dataset_metadata(row)

        # 7. Добавляем главную запись
        self._insert_record(row, organism_id, observer_id, location_id,
                            event_id, media_id, dataset_metadata_id)

    def _insert_organism(self, row) -> str:
        """Вставка млекопитающего"""
        organism_id = str(row.get('organism_id', row.get('occurrence_id', '')))

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Organism 
            (organism_id, scientific_name, vernacular_name, taxon_rank, organism_name, sex, organism_remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            organism_id,
            row.get('scientific_name', ''),
            row.get('vernacular_name', ''),
            row.get('taxon_rank', ''),
            row.get('organism_name', ''),
            row.get('sex', ''),
            row.get('organism_remarks', '')
        ))

        return organism_id

    def _insert_observer(self, row) -> int:
        """Вставка наблюдателя"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Observer (recorded_by, institution_code)
            VALUES (?, ?)
        """, (row.get('recorded_by', ''), row.get('institution_code', '')))

        cursor.execute("SELECT observer_id FROM Observer WHERE recorded_by = ?",
                       (row.get('recorded_by', ''),))
        result = cursor.fetchone()
        return result[0] if result else None

    def _insert_location(self, row) -> int:
        """Вставка локации"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Location 
            (higher_geography, water_body, locality, verbatim_locality)
            VALUES (?, ?, ?, ?)
        """, (
            row.get('higher_geography', ''),
            row.get('water_body', ''),
            row.get('locality', ''),
            row.get('verbatim_locality', '')
        ))

        cursor.execute("""
            SELECT location_id FROM Location 
            WHERE higher_geography = ? AND water_body = ? AND locality = ? AND verbatim_locality = ?
        """, (
            row.get('higher_geography', ''),
            row.get('water_body', ''),
            row.get('locality', ''),
            row.get('verbatim_locality', '')
        ))
        result = cursor.fetchone()
        return result[0] if result else None

    def _insert_event(self, row) -> str:
        """Вставка события"""
        event_id = str(row.get('event_id', row.get('occurrence_id', '')))

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Event 
            (event_id, basis_of_record, individual_count, preparations, occurrence_remarks)
            VALUES (?, ?, ?, ?, ?)
        """, (
            event_id,
            row.get('basis_of_record', ''),
            row.get('individual_count', 0),
            row.get('preparations', ''),
            row.get('occurrence_remarks', '')
        ))

        return event_id

    def _insert_media(self, row) -> int:
        """Вставка медиа"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Media 
            (external_resource, external_resource_thumb, license, rights_holder, catalog_number)
            VALUES (?, ?, ?, ?, ?)
        """, (
            row.get('external_resource', ''),
            row.get('external_resource_thumb', ''),
            row.get('license', ''),
            row.get('rights_holder', ''),
            row.get('catalog_number', '')
        ))

        cursor.execute("""
            SELECT media_id FROM Media 
            WHERE external_resource = ? AND catalog_number = ?
        """, (row.get('external_resource', ''), row.get('catalog_number', '')))
        result = cursor.fetchone()
        return result[0] if result else None

    def _insert_dataset_metadata(self, row) -> int:
        """Вставка метаданных датасета"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO DatasetMetadata (oid, type, modified, language)
            VALUES (?, ?, ?, ?)
        """, (
            row.get('oid', ''),
            row.get('type', ''),
            row.get('modified', ''),
            row.get('language', '')
        ))

        return cursor.lastrowid

    def _insert_record(self, row, organism_id, observer_id, location_id,
                       event_id, media_id, dataset_metadata_id):
        """Вставка главной записи"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO Record 
            (occurrence_id, decimal_latitude, decimal_longitude, event_date, event_time,
             coordinate_precision, geodetic_datum, event_id, organism_id, 
             observer_id, location_id, media_id, dataset_metadata_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get('occurrence_id', ''),
            row.get('decimal_latitude', 0.0),
            row.get('decimal_longitude', 0.0),
            row.get('event_date', ''),
            row.get('event_time', ''),
            row.get('coordinate_precision', 0.0),
            row.get('geodetic_datum', ''),
            event_id,
            organism_id,
            observer_id,
            location_id,
            media_id,
            dataset_metadata_id
        ))

    def get_all_species(self) -> List[Dict]:
        """Получить все виды в базе данных"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT scientific_name, vernacular_name, taxon_rank
            FROM Organism 
            WHERE scientific_name != ''
            ORDER BY scientific_name
        """)
        return [dict(row) for row in cursor.fetchall()]

    def get_observations_by_species(self, scientific_name: str) -> List[Dict]:
        """Получить все наблюдения конкретного вида"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT r.*, o.scientific_name, o.vernacular_name, obs.recorded_by,
                   l.locality, l.water_body
            FROM Record r
            JOIN Organism o ON r.organism_id = o.organism_id
            JOIN Observer obs ON r.observer_id = obs.observer_id
            JOIN Location l ON r.location_id = l.location_id
            WHERE o.scientific_name = ?
            ORDER BY r.event_date DESC
        """, (scientific_name,))
        return [dict(row) for row in cursor.fetchall()]

    def get_observations_by_location(self, locality: str) -> List[Dict]:
        """Получить все наблюдения в конкретной локации"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT r.*, o.scientific_name, o.vernacular_name, obs.recorded_by,
                   l.locality, l.water_body
            FROM Record r
            JOIN Organism o ON r.organism_id = o.organism_id
            JOIN Observer obs ON r.observer_id = obs.observer_id
            JOIN Location l ON r.location_id = l.location_id
            WHERE l.locality LIKE ?
            ORDER BY r.event_date DESC
        """, (f'%{locality}%',))
        return [dict(row) for row in cursor.fetchall()]

    def get_observations_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """Получить наблюдения за период времени"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT r.*, o.scientific_name, o.vernacular_name, obs.recorded_by,
                   l.locality, l.water_body
            FROM Record r
            JOIN Organism o ON r.organism_id = o.organism_id
            JOIN Observer obs ON r.observer_id = obs.observer_id
            JOIN Location l ON r.location_id = l.location_id
            WHERE r.event_date BETWEEN ? AND ?
            ORDER BY r.event_date DESC
        """, (start_date, end_date))
        return [dict(row) for row in cursor.fetchall()]

    def get_statistics(self) -> Dict:
        """Получить общую статистику по базе данных"""
        cursor = self.conn.cursor()

        stats = {}

        # Общее количество записей
        cursor.execute("SELECT COUNT(*) FROM Record")
        stats['total_records'] = cursor.fetchone()[0]

        # Количество уникальных видов
        cursor.execute("SELECT COUNT(DISTINCT scientific_name) FROM Organism WHERE scientific_name != ''")
        stats['unique_species'] = cursor.fetchone()[0]

        # Количество наблюдателей
        cursor.execute("SELECT COUNT(*) FROM Observer")
        stats['total_observers'] = cursor.fetchone()[0]

        # Количество локаций
        cursor.execute("SELECT COUNT(*) FROM Location")
        stats['total_locations'] = cursor.fetchone()[0]

        # Период наблюдений
        cursor.execute("SELECT MIN(event_date), MAX(event_date) FROM Record WHERE event_date != ''")
        result = cursor.fetchone()
        stats['observation_period'] = {'start': result[0], 'end': result[1]}

        return stats

    def search_records(self, **kwargs) -> List[Dict]:
        """Универсальный поиск записей по различным параметрам"""
        conditions = []
        params = []

        if kwargs.get('species'):
            conditions.append("o.scientific_name LIKE ?")
            params.append(f"%{kwargs['species']}%")

        if kwargs.get('location'):
            conditions.append("l.locality LIKE ?")
            params.append(f"%{kwargs['location']}%")

        if kwargs.get('observer'):
            conditions.append("obs.recorded_by LIKE ?")
            params.append(f"%{kwargs['observer']}%")

        if kwargs.get('start_date'):
            conditions.append("r.event_date >= ?")
            params.append(kwargs['start_date'])

        if kwargs.get('end_date'):
            conditions.append("r.event_date <= ?")
            params.append(kwargs['end_date'])

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT r.*, o.scientific_name, o.vernacular_name, obs.recorded_by,
                   l.locality, l.water_body, l.higher_geography
            FROM Record r
            JOIN Organism o ON r.organism_id = o.organism_id
            JOIN Observer obs ON r.observer_id = obs.observer_id
            JOIN Location l ON r.location_id = l.location_id
            WHERE {where_clause}
            ORDER BY r.event_date DESC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def add_new_record(self, record_data: Dict) -> str:
        """Добавление новой записи наблюдения"""
        try:
            df = pd.DataFrame([record_data])
            row = df.iloc[0]

            self._process_csv_row(row)
            self.conn.commit()

            occurrence_id = record_data.get('occurrence_id', f"new_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            logger.info(f"Добавлена новая запись: {occurrence_id}")
            return occurrence_id

        except Exception as e:
            logger.error(f"Ошибка добавления записи: {e}")
            self.conn.rollback()
            raise

    def delete_record(self, occurrence_id: str) -> bool:
        """Удаление записи по occurrence_id"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM Record WHERE occurrence_id = ?", (occurrence_id,))

            if cursor.rowcount > 0:
                self.conn.commit()
                logger.info(f"Запись {occurrence_id} удалена")
                return True
            else:
                logger.warning(f"Запись {occurrence_id} не найдена")
                return False

        except Exception as e:
            logger.error(f"Ошибка удаления записи: {e}")
            self.conn.rollback()
            return False

    def update_record(self, occurrence_id: str, update_data: Dict) -> bool:
        """Обновление записи"""
        try:
            # Получаем текущую запись
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM Record WHERE occurrence_id = ?", (occurrence_id,))
            current_record = cursor.fetchone()

            if not current_record:
                logger.warning(f"Запись {occurrence_id} не найдена")
                return False

            # Обновляем только поля Record таблицы
            record_fields = ['decimal_latitude', 'decimal_longitude', 'event_date',
                             'event_time', 'coordinate_precision', 'geodetic_datum']

            update_fields = []
            params = []

            for field in record_fields:
                if field in update_data:
                    update_fields.append(f"{field} = ?")
                    params.append(update_data[field])

            if update_fields:
                params.append(occurrence_id)
                query = f"UPDATE Record SET {', '.join(update_fields)} WHERE occurrence_id = ?"
                cursor.execute(query, params)
                self.conn.commit()
                logger.info(f"Запись {occurrence_id} обновлена")
                return True

            return False

        except Exception as e:
            logger.error(f"Ошибка обновления записи: {e}")
            self.conn.rollback()
            return False

    def close(self):
        """Закрытие соединения с базой данных"""
        if self.conn:
            self.conn.close()
            logger.info("Соединение с базой данных закрыто")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    with DatabaseManager("db/db_whales.db") as db:

        # 1. Загрузка данных из CSV
        print("1. Загрузка данных из CSV файла...")
        db.load_csv_data("datasets/obis_seamap_dataset_1739_points.csv")

        # 2. Добавление новой записи
        print("\n2. Добавление новой записи...")
        new_record = {
            'occurrence_id': 'test_001',
            'scientific_name': 'Balaenoptera musculus',
            'vernacular_name': 'Blue Whale',
            'decimal_latitude': 34.0522,
            'decimal_longitude': -118.2437,
            'event_date': '2025-05-29',
            'event_time': '14:30:00',
            'recorded_by': 'Test Observer',
            'locality': 'Los Angeles Coast',
            'water_body': 'Pacific Ocean',
            'basis_of_record': 'HumanObservation',
            'individual_count': 1
        }
        # db.add_new_record(new_record)

        # 3. Получение статистики
        print("\n3. Статистика базы данных:")
        stats = db.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # 4. Поиск всех видов
        print("\n4. Все виды в базе данных:")
        species = db.get_all_species()
        for sp in species[:5]:  # Показываем первые 5
            print(f"  {sp['scientific_name']} ({sp['vernacular_name']})")

        # 5. Поиск наблюдений серого кита
        print("\n5. Наблюдения серого кита:")
        observations = db.get_observations_by_species('Eschrichtius robustus')
        print(f"  Найдено {len(observations)} наблюдений")

        # 6. Универсальный поиск
        print("\n6. Поиск по параметрам:")
        results = db.search_records(
            species='Gray',
            start_date='2024-01-01',
            end_date='2025-12-31'
        )
        print(f"  Найдено {len(results)} записей")

        # 7. Наблюдения в конкретной локации
        print("\n7. Наблюдения в Мексике:")
        mexico_obs = db.get_observations_by_location('Mexico')
        print(f"  Найдено {len(mexico_obs)} наблюдений")


if __name__ == "__main__":
    main()
