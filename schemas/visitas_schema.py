from pydantic import BaseModel, EmailStr, Field, validator
from typing import Optional
from datetime import datetime

class VisitaRecord(BaseModel):
    email: EmailStr
    jk: Optional[str] = None
    badmail: Optional[str] = None
    baja: Optional[str] = None
    fecha_envio: datetime
    fecha_open: Optional[datetime] = None
    opens: int = Field(ge=0)
    opens_virales: int = Field(ge=0)
    fecha_click: Optional[datetime] = None
    clicks: int = Field(ge=0)
    clicks_virales: int = Field(ge=0)
    links: Optional[str] = None
    ips: Optional[str] = None
    navegadores: Optional[str] = None
    plataformas: Optional[str] = None

    @validator('badmail')
    def validate_badmail(cls, v):
        if v and v not in ['HARD', '']:
            raise ValueError('Badmail must be HARD or empty')
        return v

    @validator('baja')
    def validate_baja(cls, v):
        if v and v not in ['SI', '']:
            raise ValueError('Baja must be SI or empty')
        return v

    @validator('ips')
    def validate_ips(cls, v):
        if v and v != '-':
            # Validate IP format (simple check for n.n.n.n)
            import re
            if not re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', v):
                raise ValueError('Invalid IP format')
            # Check each octet is 0-255
            octets = v.split('.')
            for octet in octets:
                if not 0 <= int(octet) <= 255:
                    raise ValueError('IP octet out of range')
        return v

    @validator('fecha_envio', 'fecha_open', 'fecha_click', pre=True)
    def parse_datetime(cls, v):
        if v == '-' or v is None:
            return None
        try:
            return datetime.strptime(v, '%d/%m/%Y %H:%M')
        except ValueError:
            raise ValueError(f'Invalid datetime format: {v}')

    class Config:
        allow_population_by_field_name = True