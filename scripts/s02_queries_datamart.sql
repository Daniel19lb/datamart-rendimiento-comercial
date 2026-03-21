CREATE DATABASE DM_CONTRATOS_COMERCIAL

USE DM_CONTRATOS_COMERCIAL

-- Capas
CREATE SCHEMA ods
CREATE SCHEMA stg
CREATE SCHEMA bds

--CAPA STG - Transformacion de como queremos nuestro modelo

SELECT idAsesor,nombre,estado
FROM ods.M_Asesores

CREATE TABLE stg.M_Asesores (
    Asesor_Skey INT IDENTITY (1,1) NOT NULL,
    Asesor_codigo INT,
    Asesor_Nombre VARCHAR(40),
	Asesor_Estado VARCHAR(15)
);
select * from stg.M_Asesores
SELECT idEquipo,nombreEquipo,jefeEquipo
FROM ods.M_Equipos

CREATE TABLE stg.M_Equipos (
    Equipo_Skey INT IDENTITY (1,1) NOT NULL,
	Equipo_Codigo INT,
    Equipo_Nombre VARCHAR(40),
    Equipo_Jefe VARCHAR(40)
);

SELECT idKPI,nombre,metaDiaria,metaMensual
FROM ods.M_KPIS

CREATE TABLE stg.M_KPIS (
    KPIS_Skey INT IDENTITY (1,1) NOT NULL,
    KPIS_Codigo INT,
    KPIS_Nombre VARCHAR(40),
    KPIS_MetaDiaria INT,
    KPIS_MetaMensual INT
);

--stg ventas
SELECT V.idVenta,A.idAsesor,E.idEquipo,K.idKPI,fecha,V.tipoServicio,V.cantidad
FROM ods.M_Ventas V
INNER JOIN ods.M_Asesores A ON V.idAsesor=A.idAsesor
INNER JOIN ods.M_Equipos E ON A.idEquipo=E.idEquipo
INNER JOIN ods.M_KPIS K ON K.idEquipo=E.idEquipo

CREATE TABLE stg.M_Ventas (
	Ventas_ID BIGINT,
	Asesor_Skey INT,
	Asesor_codigo INT,
	Equipo_Skey INT,
	Equipo_Codigo INT,
	KPIS_Skey INT,
	KPIS_Codigo INT,
    Ventas_Fecha DATE,
    Ventas_TipoServicio VARCHAR(20),
    Ventas_Cantidad INT
);



select * from ods.M_Asesores
select * from ods.M_Equipos
select * from ods.M_KPIS
select * from ods.M_Ventas
order by fecha

select * from stg.M_Asesores
select * from stg.M_Equipos
select * from stg.M_KPIS
select * from stg.M_Ventas
order by Ventas_Fecha asc


--truncate ods
TRUNCATE TABLE ods.M_Asesores
TRUNCATE TABLE ods.M_Equipos
TRUNCATE TABLE ods.M_KPIS


--truncate stg
TRUNCATE TABLE stg.M_Asesores
DBCC CHECKIDENT ('stg.M_Asesores', RESEED,1)

TRUNCATE TABLE stg.M_Equipos
DBCC CHECKIDENT ('stg.M_Empleados', RESEED,1)

TRUNCATE TABLE stg.M_KPIS
DBCC CHECKIDENT ('stg.M_KPIS', RESEED,1)


--Dimensiones
CREATE TABLE bds.Dim_Asesores (
Asesor_Skey int NOT NULL,
Asesor_Nombre varchar(40) NOT NULL,
Asesor_Estado varchar(15),
)

CREATE TABLE bds.Dim_Equipos (
Equipo_Skey int NOT NULL,
Equipo_Nombre varchar(40) NOT NULL,
Equipo_Jefe varchar(40),
)


CREATE TABLE bds.Dim_KPIS (
KPIS_Skey int NOT NULL,
KPIS_Nombre varchar(40) NOT NULL,
KPIS_MetaDiaria INT,
KPIS_MetaMensual INT
)

ALTER TABLE bds.Dim_KPIS
alter column KPIS_MetaDiaria INT
CREATE TABLE bds.Fact_Ventas (
Ventas_ID BIGINT NOT NULL,
Asesor_Skey INT NOT NULL,
Equipo_Skey INT NOT NULL,
KPIS_Skey INT NOT NULL,
Ventas_Fecha DATE NULL,
Ventas_TipoServicio VARCHAR(20),
Ventas_Cantidad INT
)

CREATE TABLE bds.Dim_Tiempo (
Fecha DATE NOT NULL,
FullDate DATE NOT NULL,
Anio INT,
Mes INT,
Dia INT,
DiaSemana varchar(100),
Nombre_Semana varchar(100),
Nombre_Mes varchar(100),
Semestre INT,
Trimestre INT,
Cuatrimestre INT,
Bimestre INT,
Periodo INT
)

-- Ingesta de dimension tiempo 
DECLARE @fechainicio date, @fechafin date;

ALTER TABLE bds.Fact_Ventas NOCHECK CONSTRAINT VentasDate_fk;
-- Limpia la dimensión si ya tiene datos
IF (SELECT COUNT(*) FROM bds.Dim_Tiempo) > 0
BEGIN
    DELETE FROM bds.Dim_Tiempo;
END
-- Reactiva la FK
ALTER TABLE bds.Fact_Ventas CHECK CONSTRAINT VentasDate_fk;

-- Define el rango de fechas basado en M_Ventas
SELECT 
    @fechainicio = MIN(Ventas_Fecha), 
    @fechafin = MAX(Ventas_Fecha)
FROM stg.M_Ventas;

-- Configura el lenguaje a español
SET LANGUAGE SPANISH;

-- Carga la dimensión tiempo
WHILE @fechainicio <= @fechafin
BEGIN
    INSERT INTO bds.Dim_Tiempo
    VALUES (
        CONVERT(DATE, @fechainicio),
        @fechainicio,
        YEAR(@fechainicio),
        MONTH(@fechainicio),
        DAY(@fechainicio),
        DATEPART(WEEKDAY, @fechainicio),
        DATENAME(WEEKDAY, @fechainicio),
        DATENAME(MONTH, @fechainicio),
        ((MONTH(@fechainicio)-1)/6)+1,
        DATEPART(QUARTER, @fechainicio),
        ((MONTH(@fechainicio)-1)/4)+1,
        ((MONTH(@fechainicio)-1)/2)+1,
        CONVERT(INT, LEFT(CONVERT(varchar(10), @fechainicio, 112), 6)) --YYYYMM
    );

    SET @fechainicio = DATEADD(DAY, 1, @fechainicio);
END

-- Validación
SELECT * FROM bds.Dim_Tiempo;

ALTER TABLE bds.Dim_Tiempo ADD CONSTRAINT FullDate_pk PRIMARY KEY (FullDate)
ALTER TABLE bds.Dim_Asesores ADD CONSTRAINT Asesor_pk PRIMARY KEY (Asesor_Skey)
ALTER TABLE bds.Dim_Equipos ADD CONSTRAINT Equipo_pk PRIMARY KEY (Equipo_Skey)
ALTER TABLE bds.Dim_KPIS ADD CONSTRAINT KPIS_pk PRIMARY KEY (KPIS_Skey)


ALTER TABLE bds.Fact_Ventas ADD CONSTRAINT VentasDate_fk FOREIGN KEY (Ventas_Fecha)
      REFERENCES bds.Dim_Tiempo (FullDate)

ALTER TABLE bds.Fact_Ventas ADD CONSTRAINT Asesor_fk FOREIGN KEY (Asesor_Skey)
      REFERENCES bds.Dim_Asesores (Asesor_Skey)

ALTER TABLE bds.Fact_Ventas ADD CONSTRAINT Equipo_fk FOREIGN KEY (Equipo_Skey)
      REFERENCES bds.Dim_Equipos (Equipo_Skey)

ALTER TABLE bds.Fact_Ventas ADD CONSTRAINT KPIS_fk FOREIGN KEY (KPIS_Skey)
      REFERENCES bds.Dim_KPIS (KPIS_Skey)

SELECT * FROM bds.Dim_Asesores
SELECT * FROM bds.Dim_Equipos
SELECT * FROM bds.Dim_KPIS
SELECT * FROM bds.Fact_Ventas
order by Asesor_Skey asc

SELECT DISTINCT fv.Asesor_Skey
FROM bds.Fact_Ventas fv
LEFT JOIN bds.Dim_Asesores da ON fv.Asesor_Skey = da.Asesor_Skey
WHERE da.Asesor_Skey IS NULL;



TRUNCATE TABLE stg.M_Asesores
DBCC CHECKIDENT ('stg.M_Asesores', RESEED,1)

TRUNCATE TABLE stg.M_Equipos
DBCC CHECKIDENT ('stg.M_Equipos', RESEED,1)

TRUNCATE TABLE stg.M_KPIS
DBCC CHECKIDENT ('stg.M_KPIS', RESEED,1)

TRUNCATE TABLE stg.M_Ventas

SELECT * FROM bds.Fact_Ventas
Where Ventas_Fecha BETWEEN '2025-07-01' AND '2025-07-31'
Order by Asesor_Skey

select * from bds.Fact_Ventas

DELETE 
  FROM bds.Fact_Ventas
WHERE Ventas_Fecha BETWEEN '2025-06-01' AND '2025-06-30'
  AND Asesor_Skey BETWEEN 1 AND 15;


DELETE 
  FROM bds.Fact_Ventas
WHERE Ventas_Fecha BETWEEN '2025-07-01' AND '2025-07-31'
  AND Asesor_Skey BETWEEN 1 AND 15;