SELECT
    id_sucursal,
    nombre,
    ciudad,
    departamento
FROM {{ ref('stg_oltp_sucursal') }}