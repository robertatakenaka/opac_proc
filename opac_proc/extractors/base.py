# coding: utf-8
import os
import sys

from opac_proc.extractors.source_clients.amapi_wrapper import custom_amapi_client
from opac_proc.datastore.mongodb_connector import get_db_connection
from opac_proc.datastore.base_mixin import ProcessMetada

PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(PROJECT_PATH)

from opac_proc.web import config
from opac_proc.logger_setup import getMongoLogger

if config.DEBUG:
    logger = getMongoLogger(__name__, "DEBUG", "extract")
else:
    logger = getMongoLogger(__name__, "INFO", "extract")


class BaseExtractor(object):
    _db = None
    articlemeta = None
    get_instance_query = None  # definir na subclasse
    get_identifier_query = None  # definir na subclasse. dict com filtros (lookup) para recuperar o IdModel
    _raw_data = {}
    extract_model_class = None
    extract_model_name = ''
    extract_model_instance = None

    ids_model_class = None
    ids_model_name = ''
    ids_model_instance = None

    metadata = {
        'updated_at': None,
        'process_start_at': None,
        'process_finish_at': None,
        'process_completed': True,
    }

    def __init__(self):
        self._db = get_db_connection()
        self.articlemeta = custom_amapi_client.ArticleMeta(
            config.ARTICLE_META_THRIFT_DOMAIN,
            config.ARTICLE_META_THRIFT_PORT,
            config.ARTICLE_META_THRIFT_TIMEOUT)

    def get_identifier_model_instance(self):
        if not self.get_identifier_query or not isinstance(self.get_identifier_query, dict):
            raise ValueError("Deve definir self.get_identifier_query como dicionario no __init__ da subclasse")

        try:
            instance = self.ids_model_class.objects(**self.get_identifier_query)
        except Exception, e:  # does not exist or multiple objects returned
            # se existe deveria ser só uma instância do modelo
            raise e
        else:
            if not instance:
                return None
            elif instance.count() > 1:
                raise ValueError("self.get_instance_query retornou muitos resultados")
            else:
                return instance.first()

    def get_extract_model_instance(self):
        if not self.get_instance_query or not isinstance(self.get_instance_query, dict):
            raise ValueError("Deve definir self.get_instance_query como dicionario no __init__ da subclasse")

        try:
            instance = self.extract_model_class.objects(**self.get_instance_query)
        except Exception, e:  # does not exist or multiple objects returned
            # se existe deveria ser só uma instância do modelo
            raise e
        else:
            if not instance:
                return None
            elif instance.count() > 1:
                raise ValueError("self.get_instance_query retornou muitos resultados")
            else:
                return instance.first()

    def extract(self):
        """
        Conecta com a fonte (AM) e extrai todos os dados.

        Redefinir na subclasse:
        class FooExtractor(BaseExtractor):
            extract_model_class = Foo

            def __init__(self, args, kwargs):
                super(FooExtractor, self).__init__()
                # seu codigo aqui ...

            @update_metadata   <---- IMPORANTE !!!
            def extract(self):
                super(FooExtractor, self).extract()
                # seu codigo aqui ...

            def save(self):
                # implmementar só se for algo deferente

        """
        # Deve implementar a extração na subclase,
        # invocando este metodo como mostra a docstring
        raise NotImplementedError

    def save(self):
        """
        Salva os dados coletados no datastore (mongo)
        """
        logger.debug(u"Inciando metodo save()")
        if self.extract_model_class is None or self.extract_model_name is None:
            msg = u"atributos extract_model_class ou extract_model_name não forma definidos na subclasse"
            logger.error(msg)
            raise Exception(msg)
        elif self.metadata['process_start_at'] is None:
            msg = u"não foi definida o timestamp de inicio, você definiu/invocou o metodo: extract() na subclasse?"
            logger.error(msg)
            raise Exception(msg)
        elif not self._raw_data:
            msg = u"os dados coletados estão vazios, você definiu/invocou o metodo: extract() na subclasse?"
            logger.error(msg)
            raise Exception(msg)
        elif not isinstance(self._raw_data, dict):
            msg = u"os dados extraidos, não são do tipo esperado: dict()"
            logger.error(msg)
            raise Exception(msg)
        else:
            # obtemos a instância do modelo identifier:
            self.ids_model_instance = self.get_identifier_model_instance()
            if not self.ids_model_instance:
                raise ValueError('Não encontramos um modelo identifier (%s) relaciondo o esta modelo' % self.ids_model_name)
            # setamos o valor do campo UUID:
            self._raw_data['uuid'] = self.ids_model_instance.uuid
            # atualizamos as datas no self.metadata
            self.metadata['must_reprocess'] = False
            self._raw_data['metadata'] = ProcessMetada(**self.metadata)
            self.extract_model_instance = self.get_extract_model_instance()
            # salvamos no mongo
            try:
                if self.extract_model_instance:
                    logger.debug(u"extract_model_instance encontrado. Atualizando!")
                    self.extract_model_instance.modify(**self._raw_data)
                else:
                    logger.debug(u"extract_model_instance NÃO encontrado. Criando novo!")
                    self.extract_model_instance = self.extract_model_class(**self._raw_data)
                    self.extract_model_instance.save()
            except Exception, e:
                msg = u"Não foi possível salvar %s. Exceção: %s" % (
                    self.extract_model_name, e)
                logger.error(msg)
                raise e
            else:
                logger.debug(u"Reload de extract_model_instance")
                self.extract_model_instance.reload()
                logger.debug(u"Fim metodo save(), retornamos uuid: %s" % self.extract_model_instance.uuid)
                return self.extract_model_instance
