import pandas as pd
import requests
import oracledb
import config  # Arquivo de configurações do projeto (autenticação JIRA e Oracle)
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_issues(start_at=0, max_results=50):
    params = {
        'startAt': start_at,
        'maxResults': max_results,
        'jql': 'project=ITSM and KEY=ITSM-18451',
        'fields': ''
    }
    try:
        response = requests.get(config.JIRA_URL, headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }, auth=(config.JIRA_USER, config.JIRA_API_TOKEN), params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch issues: {e}")
        return None

def fetch_all_issues():
    start_at = 0
    max_results = 50
    issues = []

    while True:
        data = fetch_issues(start_at, max_results)
        if not data:
            break
        issues.extend(data['issues'])
        start_at += max_results
        if start_at >= data['total']:
            break
    return issues

def convert_date(date_str):
    if date_str:
        try:
            return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError as e:
            logging.warning(f"Date conversion error for '{date_str}': {e}")
    return None

def save_to_oracle(df):
    dsn = oracledb.makedsn(config.ORA_host, config.ORA_port, service_name=config.ORA_service_name)
    with oracledb.connect(user=config.ORA_user, password=config.ORA_password, dsn=dsn) as connection:
        with connection.cursor() as cursor:
            # Verifica se a tabela existe antes de tentar criá-la
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE DATA_MGMT.JSM (
                    id VARCHAR2(50),
                    key VARCHAR2(50),
                    created_date TIMESTAMP WITH TIME ZONE, 
                    summary VARCHAR2(1000),
                    status VARCHAR2(50),
                    zendesk_old VARCHAR(50),
                    displayName VARCHAR2(100),
                    closed_date TIMESTAMP WITH TIME ZONE, 
                    analyst VARCHAR2(100),
                    time VARCHAR2(50)                   
                    )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """)

            # Lista para armazenar os dados a serem inseridos
            rows_to_insert = []
            for index, row in df.iterrows():
                try:
                    created_date = convert_date(row['fields.created'])
                    closed_date = convert_date(row.get('fields.resolutiondate', ''))

                    logging.info(
                        f"Preparing to insert row {index} - created_date: {created_date}, closed_date: {closed_date}")

                    # Tratamento seguro para cada campo
                    summary = row.get('fields.summary', '')
                    status_name = row.get('fields.status.name', '')
                    customfield_11094 = row.get('fields.customfield_11094', '')
                    creator_display_name = row.get('fields.creator.displayName', '')
                    assignee_display_name = row.get('fields.assignee.displayName', None)  # Aqui foi feito o ajuste
                    time = row.get('fields.customfield_11081.value', None)

                    rows_to_insert.append((
                        row['id'],
                        row['key'],
                        created_date,
                        summary,
                        status_name,
                        customfield_11094,
                        creator_display_name,
                        closed_date,
                        assignee_display_name,
                        time
                    ))

                except Exception as e:
                    logging.error(f"Error processing row {index}: {e}")

            # Inserção em lote
            if rows_to_insert:
                cursor.executemany(f"""
                MERGE INTO DATA_MGMT.JSM i
                USING (
                    SELECT :1 id, :2 key, :3 created_date, :4 summary, :5 status, :6 zendesk_old,
                     :7 displayName, :8 closed_date, :9 analyst, :10 time FROM dual) d
                ON (i.id = d.id)
                WHEN MATCHED THEN
                    UPDATE SET 
                        i.key = d.key, 
                        i.created_date = d.created_date, 
                        i.summary = d.summary, 
                        i.status = d.status, 
                        i.zendesk_old = d.zendesk_old, 
                        i.displayName = d.displayName, 
                        i.closed_date = d.closed_date, 
                        i.analyst = d.analyst,
                        i.time = d.time
                WHEN NOT MATCHED THEN
                    INSERT (id, key, created_date, summary, status, zendesk_old, 
                    displayName, closed_date, analyst, time)
                    VALUES (d.id, d.key, d.created_date, d.summary, d.status, d.zendesk_old, 
                    d.displayName, d.closed_date, d.analyst, d.time)
                """, rows_to_insert)

            connection.commit()

if __name__ == "__main__":
    issues = fetch_all_issues()
    if issues:
        df = pd.json_normalize(issues, sep='.')
        try:
            # Extraindo e tratando a coluna 'fields.assignee.displayName'
            df['fields.assignee.displayName'] = df['fields.assignee'].apply(
                lambda x: x['displayName'] if isinstance(x, dict) and 'displayName' in x else None
            )

            # Agora você pode incluir diretamente a nova coluna ao invés de 'fields.assignee'
            df = df[['id', 'key', 'fields.created', 'fields.summary', 'fields.status.name',
                      'fields.customfield_11094', 'fields.creator.displayName',
                      'fields.resolutiondate',
                      'fields.assignee.displayName',  # Use a nova coluna aqui
                      'fields.customfield_11081.value']]

            logging.info("Data fetched successfully.")
            print(df.head())

            # Salvar como CSV
            df.to_csv('JSM.csv', index=False)

            # Salvar no Oracle
            save_to_oracle(df)
        except KeyError as e:
            logging.error(f"KeyError: {e}")
    else:
        logging.warning("No issues fetched.")
