CREATE DATABASE IF NOT EXISTS visitas_db;
USE visitas_db;

CREATE TABLE IF NOT EXISTS visitante (
    email VARCHAR(255) PRIMARY KEY,
    fechaPrimeraVisita DATETIME,
    fechaUltimaVisita DATETIME,
    visitasTotales INT DEFAULT 0,
    visitasAnioActual INT DEFAULT 0,
    visitasMesActual INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS estadistica (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255),
    jyv VARCHAR(255),
    badmail VARCHAR(50),
    baja VARCHAR(50),
    fecha_envio DATETIME,
    fecha_open DATETIME,
    opens INT DEFAULT 0,
    opens_virales INT DEFAULT 0,
    fecha_click DATETIME,
    clicks INT DEFAULT 0,
    clicks_virales INT DEFAULT 0,
    links TEXT,
    ips VARCHAR(255),
    navegadores TEXT,
    plataformas VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS errores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    row_index INT,
    data TEXT,
    error_message TEXT,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);