SELECT
    id_marca,
    nullif(trim(nombre), '') as nombre,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('oltp', 'marca') }}