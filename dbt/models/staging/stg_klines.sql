with source as (
    select * from {{ source('raw', 'klines') }}
),

renamed as (
    select
        open_time as open_time_raw,
        open as open_price,
        high as high_price,
        low as low_price,
        close as close_price,
        volume,
        close_time as close_time_raw,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume,
        symbol,
        interval
    from source
)

select * from renamed
