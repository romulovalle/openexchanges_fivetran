import requests
from datetime import datetime
from google.cloud import bigquery

# Configurações do BigQuery
client = bigquery.Client()

# Função para inserir linhas no BigQuery
async def insert_rows_as_stream(rows):
    dataset_id = "etl_fivetran_woebot_openexchange"
    table_id = "openexchange"
    await client.insert_rows_json(f"{dataset_id}.{table_id}", rows)
    print(f"Inserted {len(rows)} rows")

# Função de resposta
async def with_response():
    print("In withResponse")
    
    etl_fivetran_mode = [
        {
            "id": "285",
            "displayName": "Aaron Lasseigne",
            "fullName1": "Aaron Lasseigne",
            "fullName2": "Lasseigne, Aaron",
            "hireDate": "2021-04-06",
            "department": "Engineering",
            "payRateEffectiveDate": "2022-01-01",
            "payRate": "209000.00 USD",
            "payChangeReason": "Merit",
            "customDate1": None,
            "customAmount": " USD",
            "customChangeReason": None
        }
    ]

    return {
        "state": {"since_id": None},
        "insert": {"etl_fivetran_mode": etl_fivetran_mode},
        "schema": {"etl_fivetran_mode": {"primaryKey": ["id"]}},
        "hasMore": False
    }

# Handler principal
async def handler(req, res):
    dataset_id = "etl_fivetran_woebot_openexchange"
    table_id = "openexchange"
    project_id = "rvna-et"
    first_url = "https://openexchangerates.org/api/latest.json"
    second_url = "39cbf04cdf674389a50ddabe058804e2"
    base_currency_eur = "EUR"
    base_currency_usd = "USD"
    currency_arr = []

    date = datetime.now()
    year = date.year
    month = str(date.month).zfill(2)
    day_formatted = str(date.day).zfill(2)

    api_url_with_date_eur = f"{first_url}?app_id={second_url}&base={base_currency_eur}"
    api_url_with_date_usd = f"{first_url}?app_id={second_url}&base={base_currency_usd}"

    schema = [
        {"name": "year", "type": "NUMERIC"},
        {"name": "period", "type": "NUMERIC"},
        {"name": "day", "type": "NUMERIC"},
        {"name": "usd", "type": "NUMERIC"},
        {"name": "eur", "type": "NUMERIC"},
    ]

    try:
        eur_response = requests.get(api_url_with_date_eur).json()
        usd_response = requests.get(api_url_with_date_usd).json()

        eur_rate = eur_response["rates"]["USD"]
        usd_rate = usd_response["rates"]["EUR"]

        date_rates = {
            "year": year,
            "period": month,
            "day": day_formatted,
            "usd": usd_rate,
            "eur": eur_rate,
        }

        currency_arr.append(date_rates)
        rows = currency_arr
        print(rows)
        await insert_rows_as_stream(rows)
        response_data = await with_response()
        res.status(200).json(response_data)
    except Exception as e:
        print(e)
        res.status(500).send("Internal Server Error")

# Uso da função handler
handler(None, None)
