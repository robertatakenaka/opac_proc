# coding: utf-8

import os
import datetime

HERE = os.path.dirname(os.path.abspath(__file__))

# log level
OPAC_PROC_LOG_LEVEL = os.environ.get('OPAC_PROC_LOG_LEVEL', 'INFO')
# caminho absoluto (default) para o arquivo de log
OPAC_PROC_LOG_FILE_PATH_DEFAULT = '%s/logs/%s.log' % (
    HERE, datetime.datetime.now().strftime('%Y-%m-%d'))

# caminho absoluto para o arquivo de log
OPAC_PROC_LOG_FILE_PATH = os.environ.get(
    'OPAC_PROC_LOG_FILE_PATH',
    OPAC_PROC_LOG_FILE_PATH_DEFAULT)

# host e porta para conectar na API Thrift do Article meta
ARTICLE_META_THRIFT_DOMAIN = os.environ.get(
    'OPAC_PROC_ARTICLE_META_THRIFT_DOMAIN',
    'articlemeta.scielo.org')
ARTICLE_META_THRIFT_PORT = int(os.environ.get(
    'OPAC_PROC_ARTICLE_META_THRIFT_PORT',
    11621))

# caminho absoluto para os XMLs do artigo, caminho até o acrônimo do periódico.
ARTICLE_FILE_PATH = os.environ.get('OPAC_PROC_ARTICLE_FILE_PATH', 'articles_scielosp_org')

# coleção a ser processada
OPAC_PROC_COLLECTION = os.environ.get('OPAC_PROC_COLLECTION', 'spa')


# host, porta e credenciais para conectar ao MongoDB
MONGODB_NAME = os.environ.get('OPAC_PROC_MONGODB_NAME', 'opac')
MONGODB_HOST = os.environ.get('OPAC_PROC_MONGODB_HOST', 'localhost')
MONGODB_PORT = os.environ.get('OPAC_PROC_MONGODB_PORT', 27017)
MONGODB_USER = os.environ.get('OPAC_PROC_MONGODB_USER', None)
MONGODB_PASS = os.environ.get('OPAC_PROC_MONGODB_PASS', None)

MONGODB_SETTINGS = {
    'db': MONGODB_NAME,
    'host': MONGODB_HOST,
    'port': int(MONGODB_PORT),
}

if MONGODB_USER and MONGODB_PASS:
    MONGODB_SETTINGS['username'] = MONGODB_USER
    MONGODB_SETTINGS['password'] = MONGODB_PASS

# host, porta e credenciais para conectar ao MongoDB do OPAC webapp
OPAC_MONGODB_NAME = os.environ.get('OPAC_MONGODB_NAME', 'opac')
OPAC_MONGODB_HOST = os.environ.get('OPAC_MONGODB_HOST', 'localhost')
OPAC_MONGODB_PORT = os.environ.get('OPAC_MONGODB_PORT', 27017)
OPAC_MONGODB_USER = os.environ.get('OPAC_MONGODB_USER', None)
OPAC_MONGODB_PASS = os.environ.get('OPAC_MONGODB_PASS', None)

OPAC_MONGODB_SETTINGS = {
    'db': OPAC_MONGODB_NAME,
    'host': OPAC_MONGODB_HOST,
    'port': int(OPAC_MONGODB_PORT),
}

if OPAC_MONGODB_USER and OPAC_MONGODB_PASS:
    OPAC_MONGODB_SETTINGS['username'] = OPAC_MONGODB_USER
    OPAC_MONGODB_SETTINGS['password'] = OPAC_MONGODB_PASS

