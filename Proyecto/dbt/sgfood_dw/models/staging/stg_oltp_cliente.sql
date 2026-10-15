SELECT
    id_cliente,
    nullif(trim(nit), '') as nit,
    nullif(trim(nombre), '') as nombre,
    nullif(trim(tipo_cliente), '') as tipo_cliente,
    nullif(trim(municipio), '') as municipio,
    nullif(trim(departamento), '') as departamento,
    fecha_alta,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('oltp', 'cliente') }}