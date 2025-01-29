from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

conexao_str = (
    "mssql+pyodbc://RICKY\\SQLEXPRESS/coodesh?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(conexao_str)

comando = "SELECT * FROM dbo.vendas"
df = pd.read_sql_query(comando, engine)

#print(df)
df['data_venda'] = pd.to_datetime(df['data_venda'])
df['ano'] = df['data_venda'].dt.year
df['mes'] = df['data_venda'].dt.month
df['dia'] = df['data_venda'].dt.day
df['data_venda'] = df['data_venda'].dt.strftime('%Y-%m-%d')
#print(df)
df_agg = df.groupby('data_venda', as_index=False)['valor_total'].sum()
df_agg.rename(columns={'valor_total': 'total_vendas'}, inplace=True)

df_agg = df_agg.drop_duplicates()

#print(df_agg)


df_write = pa.Table.from_pandas(df)
df_write_agg = pa.Table.from_pandas(df_agg)


pq.write_to_dataset(df_write, root_path='C:\\coodesh',
                    partition_cols=['ano', 'mes', 'dia'])