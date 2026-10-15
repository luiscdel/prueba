SELECT
    id_categoria,
    nullif(trim(nombre), '') as nombre,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('oltp', 'categoria') }}