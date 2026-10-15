SELECT
    id_sucursal,
    nullif(trim(nombre), '') as nombre,
    nullif(trim(ciudad), '') as ciudad,
    nullif(trim(departamento), '') as departamento,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('oltp', 'sucursal') }}