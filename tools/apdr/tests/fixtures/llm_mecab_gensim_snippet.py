"""Japanese NLP with MeCab tokenizer and gensim.
MeCab -> mecab-python3 on PyPI. gensim -> gensim."""
import logging
import sys
import os.path
import bz2
from gensim.corpora import WikiCorpus
from gensim.corpora.wikicorpus import filterWiki
import MeCab
