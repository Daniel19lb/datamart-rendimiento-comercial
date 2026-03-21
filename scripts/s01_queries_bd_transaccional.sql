CREATE DATABASE DCONTRATOSCOMERCIAL

USE CONTRATOSCOMERCIAL

CREATE TABLE ASESOR
(idAsesor INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
idEquipo INT NOT NULL,
nombre varchar (40),
estado varchar (15)
)

CREATE TABLE EQUIPO
(idEquipo INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
nombreEquipo VARCHAR(40),
jefeEquipo VARCHAR (40),
)

CREATE TABLE KPI
(idKPI INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
idEquipo INT NOT NULL,
nombre VARCHAR(40),
metaDiaria INT,
metaMensual INT
)

CREATE TABLE VENTA
(idVenta BIGINT NOT NULL,
idAsesor INT,
fecha DATE,
tipoServicio VARCHAR(20),
cantidad int
)

-- FOREIGN ASESOR
ALTER TABLE ASESOR
ADD CONSTRAINT FK_Asesor_Equipo
FOREIGN KEY (idEquipo) REFERENCES EQUIPO(idEquipo);


-- FOREIGN KPI
ALTER TABLE KPI
ADD CONSTRAINT FK_KPI_Equipo
FOREIGN KEY (idEquipo) REFERENCES EQUIPO(idEquipo);


-- FOREIGN VENTA
ALTER TABLE VENTA
ADD CONSTRAINT FK_Venta_Asesor
FOREIGN KEY (idAsesor) REFERENCES ASESOR(idAsesor);

ALTER TABLE VENTA
ADD CONSTRAINT PK_VENTA PRIMARY KEY (idVenta);

-- INSERT EQUIPO
INSERT INTO EQUIPO VALUES('Ventas Digital', 'María Torres')
INSERT INTO EQUIPO VALUES('Fibra Hogar', 'Carlos Mendoza')

-- INSERT ASESOR
-- Equipo 1: Ventas Digital
INSERT INTO ASESOR VALUES(1, 'Valeria Quispe', 'activo')
INSERT INTO ASESOR VALUES(1, 'Rodrigo Huanca', 'activo')
INSERT INTO ASESOR VALUES(1, 'Camila Paredes', 'activo')
INSERT INTO ASESOR VALUES(1, 'Diego Vargas', 'activo')
INSERT INTO ASESOR VALUES(1, 'Lucia Flores', 'activo')
INSERT INTO ASESOR VALUES(1, 'Andres Solis', 'activo')
INSERT INTO ASESOR VALUES(1, 'Gabriela Ramos', 'activo')
INSERT INTO ASESOR VALUES(1, 'Franco Perez', 'activo')
INSERT INTO ASESOR VALUES(1, 'Natalia Campos', 'activo')
INSERT INTO ASESOR VALUES(1, 'Sebastian Cruz', 'activo')
INSERT INTO ASESOR VALUES(1, 'Paola Mamani', 'activo')
INSERT INTO ASESOR VALUES(1, 'Jorge Tapia', 'activo')
INSERT INTO ASESOR VALUES(1, 'Alessandra Vidal', 'activo')
INSERT INTO ASESOR VALUES(1, 'Marco Huamani', 'activo')
INSERT INTO ASESOR VALUES(1, 'Fiorella Casas', 'baja')

-- Equipo 2: Fibra Hogar
INSERT INTO ASESOR VALUES(2, 'Bruno Salinas', 'activo')
INSERT INTO ASESOR VALUES(2, 'Xiomara Delgado', 'activo')
INSERT INTO ASESOR VALUES(2, 'Rafael Cáceres', 'activo')
INSERT INTO ASESOR VALUES(2, 'Stephanie Mora', 'activo')
INSERT INTO ASESOR VALUES(2, 'Alonso Becerra', 'activo')

-- INSERT KPI
-- Meta diaria y mensual realistas para contact center de telecomunicaciones
-- Equipo Digital: meta de 3 ventas/día por asesor = 98 diarias / 2,156 mensuales aprox
INSERT INTO KPI VALUES(1, 'Conversion Ventas Digital', 95, 2090)
-- Equipo Fibra: meta de 2 ventas/día por asesor = 10 diarias / 220 mensuales aprox
INSERT INTO KPI VALUES(2, 'Conversion Fibra Hogar', 12, 264)


SELECT a.idAsesor,e.nombreEquipo,a.nombre,e.jefeEquipo
FROM ASESOR A INNER JOIN EQUIPO E
ON A.idEquipo=e.idEquipo

SELECT * FROM KPI



USE CONTRATOSCOMERCIAL;
GO

CREATE OR ALTER PROCEDURE sp_CargarVentasDesdeCSV
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFORMAT dmy;  -- Importante para interpretar fechas dd/MM/yyyy correctamente

    BEGIN TRY
        -- Crear tabla temporal para carga desde CSV
        CREATE TABLE #VentasTemp (
            idVenta BIGINT,
            idAsesor INT,
            fecha VARCHAR(50),  -- Texto temporalmente, se convierte más adelante
            tipoServicio VARCHAR(20),
            cantidad INT
        );

        -- Cargar CSV en la tabla temporal
        BULK INSERT #VentasTemp
        FROM 'D:\VENTA.csv'  -- ⚠️ Ajusta la ruta al archivo CSV
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK,
            MAXERRORS = 1000,
            ERRORFILE = 'D:\errores_bulk.txt'
        );

        -- Validar fechas inválidas
        SELECT *
        INTO #ErroresFecha
        FROM #VentasTemp
        WHERE TRY_CONVERT(DATE, fecha, 103) IS NULL;

        IF EXISTS (SELECT 1 FROM #ErroresFecha)
        BEGIN
            PRINT '⚠️ Existen filas con formato de fecha inválido:';
            SELECT * FROM #ErroresFecha;
            -- Aquí puedes decidir si interrumpes o continúas
        END

        ;WITH FiltradoValidas AS (
            SELECT
                idVenta,
                idAsesor,
                TRY_CONVERT(DATE, fecha, 103) AS fecha,  -- Conversión explícita dd/MM/yyyy
                tipoServicio,
                cantidad
            FROM #VentasTemp
            WHERE TRY_CONVERT(DATE, fecha, 103) IS NOT NULL
        )
        MERGE VENTA AS target
        USING FiltradoValidas AS source
        ON target.idVenta = source.idVenta
        WHEN MATCHED THEN 
            UPDATE SET 
                target.idAsesor = source.idAsesor,
                target.fecha = source.fecha,
                target.tipoServicio = source.tipoServicio,
                target.cantidad = source.cantidad
        WHEN NOT MATCHED THEN
            INSERT (idVenta, idAsesor, fecha, tipoServicio, cantidad)
            VALUES (source.idVenta, source.idAsesor, source.fecha, source.tipoServicio, source.cantidad);

        -- Limpieza de tablas temporales
        DROP TABLE IF EXISTS #VentasTemp;
        DROP TABLE IF EXISTS #ErroresFecha;

    END TRY
    BEGIN CATCH
        PRINT '⚠️ Error al cargar o procesar los datos del archivo CSV.';
        PRINT ERROR_MESSAGE();
        IF OBJECT_ID('tempdb..#VentasTemp') IS NOT NULL DROP TABLE #VentasTemp;
        IF OBJECT_ID('tempdb..#ErroresFecha') IS NOT NULL DROP TABLE #ErroresFecha;
    END CATCH
END;



EXEC sp_CargarVentasDesdeCSV;

select * from bds.fact_ventas
WHERE ventas_tipoServicio = 'TOTAL'
WHERE fecha BETWEEN '2025-06-01' AND '2025-06-30'
order by idAsesor;

select * from VENTA
order by idAsesor, fecha

-- Opción A: usando BETWEEN (incluye ambos extremos)
DELETE 
  FROM VENTA
WHERE fecha BETWEEN '2025-06-01' AND '2025-06-30'
  AND idAsesor BETWEEN 1 AND 15;


DELETE 
  FROM VENTA
WHERE fecha BETWEEN '2025-07-01' AND '2025-07-31'
  AND idAsesor BETWEEN 1 AND 15;
