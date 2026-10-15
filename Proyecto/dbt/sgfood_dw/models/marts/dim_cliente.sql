SELECT
    id_cliente,
    nit,
    nombre,
    tipo_cliente,
    municipio,
    departamento,
    fecha_alta
FROM {{ ref('stg_oltp_cliente') }}