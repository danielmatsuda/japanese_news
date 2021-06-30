from wordcloud import WordCloud
import pandas as pd

df = pd.read_csv('../5000_words.csv')
# create dictionaries with chosen word frequency rankings
first_fifty = df[:50].set_index('word').to_dict()['count']
second_fifty = df[50:100].set_index('word').to_dict()['count']
last_fifty = df[4949:].set_index('word').to_dict()['count']

# Acknowledgement: The following line of code is adapted from Github user huideyeren's gist:
# https://gist.github.com/huideyeren/77ad128ef08d3735f17e70a38cda476b
fpath = 'C:/Windows/Fonts/UDDigiKyokashoN-R.ttc'  # Here, I just used one of my existing fonts in Windows

# generate word clouds and save them
for name, words in (('ff', first_fifty), ('sf', second_fifty), ('lf', last_fifty)):
    word_cloud = WordCloud(font_path=fpath, background_color='white',
                        width=1600, height=800, min_font_size=8).generate_from_frequencies(words)
    filename = name + 'wordcloud.png'
    word_cloud.to_file(filename)
