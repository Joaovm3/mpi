import pandas as pd
import re
import json  # Import necessário para formatação avançada, se desejado

# Função para remover vírgulas de todas as colunas
def remove_commas_in_columns(df):
  return df.map(lambda x: re.sub(r',', '', str(x)))

def clean_lyrics(text):
  # Remove colchetes [], parênteses () e chaves {}
  cleaned_text = re.sub(r'[\[\]\(\)\{\}]', '', text)
  # Remove outros caracteres especiais, exceto letras, números, espaços e o apóstrofo
  cleaned_text = re.sub(r"[^a-zA-Z0-9\s']", '', cleaned_text)
  return cleaned_text

df = pd.read_csv('spotify_millsongdata.csv', sep=',', quotechar='"')

df_cleaned = remove_commas_in_columns(df)

if 'text' in df_cleaned.columns:
    df_cleaned['text'] = df_cleaned['text'].apply(clean_lyrics)

# Exportar para JSON formatado e indentado
df_cleaned.to_json('parte-3/dados_com_etl.json', orient='records', indent=4, force_ascii=False)

print(df_cleaned.head(4))
