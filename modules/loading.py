import mysql.connector
import logging
import zipfile
import os
import shutil
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class MySQLLoader:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        logger.info("Connected to MySQL database")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from MySQL database")

    def execute_query(self, query: str, params: tuple = None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()

    def load_visitante(self, records: List[Dict[str, Any]]):
        """Load into visitante table - incremental update"""
        for record in records:
            email = record['email']
            fecha_envio = record['fecha_envio']

            # Check if email exists
            cursor = self.connection.cursor()
            cursor.execute("SELECT fechaUltimaVisita, visitasTotales FROM visitante WHERE email = %s", (email,))
            result = cursor.fetchone()

            if result:
                # Update existing
                fecha_ultima, visitas_totales = result
                if fecha_envio > fecha_ultima:
                    nueva_fecha_primera = result[0]  # keep original
                    nueva_fecha_ultima = fecha_envio
                    nuevas_visitas_totales = visitas_totales + 1
                    # Calculate visitasAnioActual, visitasMesActual - simplified
                    cursor.execute("""
                        UPDATE visitante
                        SET fechaUltimaVisita = %s, visitasTotales = %s
                        WHERE email = %s
                    """, (nueva_fecha_ultima, nuevas_visitas_totales, email))
            else:
                # Insert new
                cursor.execute("""
                    INSERT INTO visitante (email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual)
                    VALUES (%s, %s, %s, 1, 1, 1)
                """, (email, fecha_envio, fecha_envio))
            cursor.close()

    def load_estadistica(self, records: List[Dict[str, Any]]):
        """Load into estadistica table - append"""
        cursor = self.connection.cursor()
        for record in records:
            cursor.execute("""
                INSERT INTO estadistica (
                    email, jyv, badmail, baja, fecha_envio, fecha_open, opens, opens_virales,
                    fecha_click, clicks, clicks_virales, links, ips, navegadores, plataformas
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['email'], record.get('jk'), record.get('badmail'), record.get('baja'),
                record['fecha_envio'], record.get('fecha_open'), record['opens'], record['opens_virales'],
                record.get('fecha_click'), record['clicks'], record['clicks_virales'],
                record.get('links'), record.get('ips'), record.get('navegadores'), record.get('plataformas')
            ))
        self.connection.commit()
        cursor.close()
        logger.info(f"Loaded {len(records)} records into estadistica")

    def load_errores(self, errors: List[Dict[str, Any]]):
        """Load errors into errores table - append"""
        cursor = self.connection.cursor()
        for error in errors:
            cursor.execute("""
                INSERT INTO errores (row_index, data, error_message, processed_at)
                VALUES (%s, %s, %s, %s)
            """, (error['row'], str(error['data']), error['error'], datetime.now()))
        self.connection.commit()
        cursor.close()
        logger.info(f"Loaded {len(errors)} errors into errores")

    def backup_files(self, file_paths: List[str], backup_dir: str = './backups'):
        """Create zip backup of processed files and remove originals"""
        if not file_paths:
            return

        # Create backup directory
        Path(backup_dir).mkdir(parents=True, exist_ok=True)

        # Create zip filename with date
        date_str = datetime.now().strftime('%Y%m%d')
        zip_filename = f'visitas_backup_{date_str}.zip'
        zip_path = os.path.join(backup_dir, zip_filename)

        # Create zip file
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_paths:
                if os.path.exists(file_path):
                    # Add file to zip with relative path
                    arcname = os.path.basename(file_path)
                    zipf.write(file_path, arcname)
                    # Remove original file
                    os.remove(file_path)
                    logger.info(f"Backed up and removed: {file_path}")

        logger.info(f"Backup created: {zip_path}")

    def load_data(self, valid_records: List[Dict[str, Any]], error_records: List[Dict[str, Any]], file_paths: List[str] = None):
        """Main loading method"""
        try:
            self.connect()
            self.load_visitante(valid_records)
            self.load_estadistica(valid_records)
            self.load_errores(error_records)

            # Create backup after successful load
            if file_paths:
                self.backup_files(file_paths)

        finally:
            self.disconnect()