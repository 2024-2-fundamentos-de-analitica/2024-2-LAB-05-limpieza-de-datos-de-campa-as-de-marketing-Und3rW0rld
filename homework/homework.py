import pandas as pd
import zipfile
from pathlib import Path

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    def process_client_data(df):
        """Procesa los datos de clientes y aplica transformaciones necesarias."""
        df = df.copy()
        df["job"] = df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
        df["education"] = df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
        df["credit_default"] = df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
        df["mortgage"] = df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
        return df

    def process_campaign_data(df):
        """Procesa los datos de campañas y aplica transformaciones necesarias."""
        df = df.copy()
        df["previous_outcome"] = df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
        df["campaign_outcome"] = df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
        df["last_contact_date"] = pd.to_datetime(df["day"].astype(str) + '-' + df["month"].astype(str) + '-2022', errors='coerce', format='%d-%b-%Y')
        df.drop(columns=["day", "month"], inplace=True)
        return df

    input_folder = Path("files/input/")
    output_folder = Path("files/output/")
    output_folder.mkdir(parents=True, exist_ok=True)
    
    client_data = []
    campaign_data = []
    economics_data = []
    
    # Procesar cada archivo ZIP en la carpeta de entrada
    for zip_path in input_folder.glob("*.zip"):  # Procesar archivos bank-marketing-campaing-*.csv.zip
        with zipfile.ZipFile(zip_path, 'r') as archive:
            for file_name in archive.namelist():
                with archive.open(file_name) as file:
                    df = pd.read_csv(file)
                    print(f"Procesando archivo: {file_name}, Columnas disponibles: {list(df.columns)}")
                    
                    if "mortgage" in df.columns:
                        df.rename(columns={"mortgage": "mortgage"}, inplace=True)
                    
                    client_columns = ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
                    campaign_columns = ["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]
                    economics_columns = ["client_id", "cons_price_idx", "euribor_three_months"]
                    
                    client_data.append(process_client_data(df[client_columns]))
                    campaign_data.append(process_campaign_data(df[campaign_columns]))
                    economics_data.append(df[economics_columns].copy())
    
    if client_data:
        pd.concat(client_data).to_csv(output_folder / "client.csv", index=False)
    if campaign_data:
        pd.concat(campaign_data).to_csv(output_folder / "campaign.csv", index=False)
    if economics_data:
        pd.concat(economics_data).to_csv(output_folder / "economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()