import pandas as pd

data = pd.read_json('resultado.json', typ='dictionary')


df_artists = pd.DataFrame(data['artist_counts'])
df_words = pd.DataFrame(data['word_counts'])

print("Contagem de artistas:", len(df_artists))
print("Contagem de palavras distintas:", len(df_words))