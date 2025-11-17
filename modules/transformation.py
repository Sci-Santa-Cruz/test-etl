import pandas as pd
import logging
from typing import List, Dict, Any
from schemas.visitas_schema import VisitaRecord
from datetime import datetime
from expectations.visitas_expectations import validate_dataframe

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        self.valid_records = []
        self.error_records = []

    def load_csv(self, filepath: str) -> pd.DataFrame:
        """Load CSV file into DataFrame"""
        df = pd.read_csv(filepath, sep=',', encoding='utf-8', header=0)
        logger.info(f"Loaded {len(df)} rows from {filepath}")
        return df

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to DataFrame"""
        # Rename columns to match schema
        column_mapping = {
            'jk': 'jk',
            'Badmail': 'badmail',
            'Baja': 'baja',
            'Fecha envio': 'fecha_envio',
            'Fecha open': 'fecha_open',
            'Opens': 'opens',
            'Opens virales': 'opens_virales',
            'Fecha click': 'fecha_click',
            'Clicks': 'clicks',
            'Clicks virales': 'clicks_virales',
            'Links': 'links',
            'IPs': 'ips',
            'Navegadores': 'navegadores',
            'Plataformas': 'plataformas'
        }
        df = df.rename(columns=column_mapping)

        # Convert - to None
        df = df.replace('-', None)

        # Normalize email to lowercase
        df['email'] = df['email'].str.lower()

        # Convert numeric fields
        numeric_fields = ['opens', 'opens_virales', 'clicks', 'clicks_virales']
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).astype(int)

        # Convert datetime fields
        datetime_fields = ['fecha_envio', 'fecha_open', 'fecha_click']
        for field in datetime_fields:
            df[field] = pd.to_datetime(df[field], format='%d/%m/%Y %H:%M', errors='coerce')

        return df

    def validate_records(self, df: pd.DataFrame):
        """Validate each record with Pydantic"""
        for idx, row in df.iterrows():
            try:
                record = VisitaRecord(**row.to_dict())
                self.valid_records.append(record.dict())
            except Exception as e:
                logger.error(f"Validation error for row {idx}: {e}")
                self.error_records.append({'row': idx, 'data': row.to_dict(), 'error': str(e)})

    def deduplicate(self):
        """Remove duplicates based on email and all fields, keep latest fecha_envio"""
        if not self.valid_records:
            return

        df = pd.DataFrame(self.valid_records)
        df = df.sort_values('fecha_envio', ascending=False)
        df = df.drop_duplicates(subset=['email'] + [col for col in df.columns if col != 'email'], keep='first')
        self.valid_records = df.to_dict('records')

    def apply_business_rules(self):
        """Apply business rules like temporal consistency"""
        if not self.valid_records:
            return

        df = pd.DataFrame(self.valid_records)

        # Rule: fecha_open >= fecha_envio
        invalid_open = df[(df['fecha_open'].notna()) & (df['fecha_open'] < df['fecha_envio'])]
        if not invalid_open.empty:
            logger.warning(f"Found {len(invalid_open)} records with invalid fecha_open")
            for idx in invalid_open.index:
                self.error_records.append({
                    'row': idx,
                    'data': df.loc[idx].to_dict(),
                    'error': 'fecha_open < fecha_envio'
                })
            df = df.drop(invalid_open.index)

        # Rule: fecha_click >= fecha_open and fecha_click <= fecha_envio (as per proposal)
        invalid_click = df[(df['fecha_click'].notna()) & ((df['fecha_click'] < df['fecha_open']) | (df['fecha_click'] > df['fecha_envio']))]
        if not invalid_click.empty:
            logger.warning(f"Found {len(invalid_click)} records with invalid fecha_click")
            for idx in invalid_click.index:
                self.error_records.append({
                    'row': idx,
                    'data': df.loc[idx].to_dict(),
                    'error': 'fecha_click invalid'
                })
            df = df.drop(invalid_click.index)

        self.valid_records = df.to_dict('records')

    def transform_file(self, filepath: str) -> Dict[str, List[Dict[str, Any]]]:
        """Main transformation method"""
        df = self.load_csv(filepath)

        # Great Expectations validation
        ge_results = validate_dataframe(df)
        if not ge_results['success']:
            logger.warning(f"Great Expectations validation failed: {ge_results['statistics']}")
            # Log failed expectations
            for result in ge_results['results']:
                if not result['success']:
                    logger.warning(f"Failed expectation: {result['expectation_config']['expectation_type']}")

        df = self.transform_dataframe(df)
        self.validate_records(df)
        self.deduplicate()
        self.apply_business_rules()

        return {
            'valid': self.valid_records,
            'errors': self.error_records,
            'ge_results': ge_results
        }