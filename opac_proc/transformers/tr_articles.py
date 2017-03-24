# coding: utf-8
import os
from datetime import datetime
import StringIO

from werkzeug.urls import url_fix
from xylose.scielodocument import Article

from opac_proc.datastore.models import (
    ExtractArticle,
    TransformArticle,
    TransformIssue,
    TransformJournal)
from opac_proc.transformers.base import BaseTransformer
from opac_proc.extractors.decorators import update_metadata
from opac_proc.web import config
from opac_proc.logger_setup import getMongoLogger
from . import source_files_handler
from . import assets_handler

if config.DEBUG:
    logger = getMongoLogger(__name__, "DEBUG", "transform")
else:
    logger = getMongoLogger(__name__, "INFO", "transform")


class ArticleTransformer(BaseTransformer):
    extract_model_class = ExtractArticle
    extract_model_instance = None

    transform_model_class = TransformArticle
    transform_model_instance = None

    source_files = None

    def get_extract_model_instance(self, key):
        # retornamos uma instancia de ExtractJounal
        # buscando pela key (=issn)
        return self.extract_model_class.objects.get(code=key)

    @update_metadata
    def transform(self):
        xylose_source = self.clean_for_xylose()
        xylose_article = Article(xylose_source)

        # aid
        uuid = self.extract_model_instance.uuid
        self.article_uuid = str(uuid)
        self.transform_model_instance['uuid'] = uuid
        self.transform_model_instance['aid'] = uuid

        self.identify_assets(xylose_article)
        self.register_assets()

        # issue
        pid = xylose_article.issue.publisher_id
        try:
            issue = TransformIssue.objects.get(pid=pid)
        except Exception, e:
            logger.error(u"TransformIssue (pid: %s) não encontrado!")
            raise e
        else:
            self.transform_model_instance['issue'] = issue.uuid

        # journal
        acronym = xylose_article.journal.acronym
        try:
            journal = TransformJournal.objects.get(acronym=acronym)
        except Exception, e:
            logger.error(u"TransformJournal (acronym: %s) não encontrado!")
            raise e
        else:
            self.transform_model_instance['journal'] = journal.uuid

        # title
        if hasattr(xylose_article, 'original_title'):
            self.transform_model_instance['title'] = xylose_article.original_title()

        # abstract_languages
        if hasattr(xylose_article, 'translated_abstracts') and xylose_article.translated_abstracts():
            self.transform_model_instance['abstract_languages'] = xylose_article.translated_abstracts().keys()

        # translated_sections
        if hasattr(xylose_article, 'translated_section') and xylose_article.translated_section():
            translated_sections = []

            for lang, title in xylose_article.translated_section().items():
                translated_sections.append({
                    'language': lang,
                    'name': title,
                })
            self.transform_model_instance['sections'] = translated_sections

        # section
        if hasattr(xylose_article, 'original_section'):
            self.transform_model_instance['section'] = xylose_article.original_section()

        # translated_titles
        if xylose_article.translated_titles():
            translated_titles = []

            for lang, title in xylose_article.translated_titles().items():
                translated_titles.append({
                    'language': lang,
                    'name': title,
                })

            self.transform_model_instance['translated_titles'] = translated_titles

        # order
        try:
            self.transform_model_instance['order'] = int(xylose_article.order)
        except ValueError, e:
            logger.error(u'xylose_article.order inválida: %s-%s' % (e, xylose_article.order))

        # doi
        if hasattr(xylose_article, 'doi'):
            self.transform_model_instance['doi'] = xylose_article.doi

        # is_aop
        if hasattr(xylose_article, 'is_aop'):
            self.transform_model_instance['is_aop'] = xylose_article.is_aop

        # created
        self.transform_model_instance['created'] = datetime.now()

        # updated
        self.transform_model_instance['updated'] = datetime.now()

        # original_language
        if hasattr(xylose_article, 'original_language'):
            self.transform_model_instance['original_language'] = xylose_article.original_language()

        # languages
        if hasattr(xylose_article, 'languages'):
            lang_set = set(xylose_article.languages() + getattr(self.transform_model_instance, 'abstract_languages', []))
            self.transform_model_instance['languages'] = list(lang_set)

        # abstract
        if hasattr(xylose_article, 'original_abstract'):
            self.transform_model_instance['abstract'] = xylose_article.original_abstract()

        # authors
        if hasattr(xylose_article, 'authors') and xylose_article.authors:
            self.transform_model_instance['authors'] = ['%s, %s' % (a['surname'], a['given_names']) for a in xylose_article.authors]

        # fulltexts -> pdfs, htmls
        if hasattr(xylose_article, 'fulltexts'):
            htmls = []
            pdfs = []
            for text, val in xylose_article.fulltexts().items():
                if text == 'html':
                    for lang, url in val.items():
                        htmls.append({
                            'type': 'html',
                            'language': lang,
                            'url': url_fix(url)
                        })
                elif text == 'pdf':
                    for lang, url in val.items():
                        pdfs.append({
                            'type': 'pdf',
                            'language': lang,
                            'url': url_fix(url)
                        })

            self.transform_model_instance['htmls'] = htmls
            self.transform_model_instance['pdfs'] = pdfs

        # pid
        if hasattr(xylose_article, 'publisher_id'):
            self.transform_model_instance['pid'] = xylose_article.publisher_id

        # fpage
        if hasattr(xylose_article, 'start_page'):
            self.transform_model_instance['fpage'] = xylose_article.start_page

        # lpage
        if hasattr(xylose_article, 'end_page'):
            self.transform_model_instance['lpage'] = xylose_article.end_page

        # elocation
        if hasattr(xylose_article, 'elocation'):
            self.transform_model_instance['elocation'] = xylose_article.elocation

        # assets
        self.transform_model_instance['htmls'] = None
        self.transform_model_instance['pdfs'] = None
        self.transform_model_instance['assets'] = {}
        self.transform_model_instance['assets']['ssm'] = assets_handler.client_status()
        self.transform_model_instance['assets']['q'] = len(self.assets_items)
        self.transform_model_instance['assets']['erros'] = self.assets_errors

        self.report_assets_sources()
        self.finish_assets_registrations()
        self.report_registered_assets()        
        return self.transform_model_instance

    def identify_assets(self, xylose_article):
        self.source_files = source_files_handler.SourceFiles(xylose_article, config.OPAC_PROC_CSS_PATH)
        self.identify_media_assets()
        self.identify_pdf_assets()
        self.identify_xml_assets()
        self.assets_items = self.media_assets_items + self.pdf_assets_items + self.xml_assets_items
        self.assets_errors = self.media_assets_errors + self.pdf_assets_errors + self.xml_assets_errors
            
    def identify_pdf_assets(self):
        self.pdf_assets_items = []
        self.pdf_assets_errors = []
        for lang, source_file in self.source_files.pdf_files.items():
            metadata = self.source_files.article_metadata.copy()
            metadata.update({'aid': self.article_uuid})
            metadata.update({'lang': lang})
            if source_file.pfile is not None:
                self.pdf_assets_items.append(assets_handler.Asset(source_file.pfile, source_file.filename, 'pdf', metadata, self.source_files.bucket_name))
            else:
                msg = u'Não foi possível ler o arquivo {} correspondente a {} {} '.format(source_file.source_location, 'pdf', lang)
                self.pdf_assets_errors.append(msg)
                logger.error(msg)

    def identify_media_assets(self):
        self.media_assets_items = []
        self.media_assets_errors = []
        for href, source_file in self.source_files.media_files.items():
            metadata = self.source_files.article_metadata.copy()
            metadata.update({'aid': self.article_uuid})
            metadata.update({'label': href})
            if source_file.pfile is not None:
                self.media_assets_items.append(assets_handler.Asset(source_file.pfile, source_file.filename, '', metadata, self.source_files.bucket_name))
            else:
                msg = u'Não foi possível ler o arquivo {} correspondente a {} {} '.format(source_file.source_location, 'media', href)
                self.media_assets_errors.append(msg)
                logger.error(msg)

    def identify_xml_assets(self):
        self.xml_assets_items = []
        self.xml_assets_errors = []
        if self.source_files.xml_file is not None:
            metadata = self.source_files.article_metadata.copy()
            metadata.update({'aid': self.article_uuid})
            metadata.update({'label': 'xml'})
            source_file = self.source_files.xml_file
            if source_file.pfile is not None:
                self.xml_assets_items.append(assets_handler.Asset(source_file.pfile, source_file.filename, 'xml', metadata, self.source_files.bucket_name))
            else:
                msg = u'Não foi possível ler o arquivo {} correspondente a {}'.format(source_file.source_location, 'xml')
                self.xml_assets_errors.append(msg)
                logger.error(msg)

    def register_assets(self):
        ssm_status = assets_handler.client_status()   
        if ssm_status is True:
            for asset in self.assets_items:
                asset.register()
        else:
            logger.error(ssm_status)

    def generate_html_assets(self):
        self.html_assets_items = []
        html_files = self.source_files.generated_html_files(self.registered_media_assets)
        if html_files is not None:
            for lang, pfile in html_files.items():
                filename = lang+'_'+self.source_files.article_folder_name + '.html'                        
                metadata = self.source_files.article_metadata.copy()
                metadata.update({'aid': self.article_uuid})
                metadata.update({'lang': lang})
                self.html_assets_items.append(assets_handler.Asset(pfile, filename, 'html', metadata, self.source_files.bucket_name))

        self.html_assets_errors = self.source_files.generated_html_errors
        for msg in self.html_assets_errors:
            logger.error(msg)

    def report_assets_sources(self):
        self.transform_model_instance['assets']['sources'] = {}
        self.transform_model_instance['assets']['sources']['pdfs'] = [source_file.source_location for source_file in self.source_files.pdf_files.values()]
        self.transform_model_instance['assets']['sources']['media'] = [source_file.source_location for source_file in self.source_files.media_files.values()]
        self.transform_model_instance['assets']['sources']['xml'] = self.source_files.xml_file.source_location if self.source_files.xml_file is not None else None

    def finish_assets_registrations(self):        
        if len(self.assets_items) > 0:
            logger.info('registration - init')
            finished = False
            make_html = len(self.xml_assets_items) > 0

            while not finished:
                
                registering_media = len([True for asset in self.media_assets_items if asset.status() == 'queued'])
                logger.info(u'Registrando {} mídias'.format(registering_media))

                if make_html and registering_media == 0:
                    self.registered_media_assets = {asset.name.replace('.', '-DOT-'): asset.url for asset in self.media_assets_items}
                    self.generate_html_assets()
                    for asset in self.html_assets_items:
                        asset.register()
                    self.assets_items.extend(self.html_assets_items)
                    make_html = False
                
                registering = len([True for asset in self.assets_items if asset.status() == 'queued'])
                logger.info(u'Registrando {} ativos'.format(registering))
                
                finished = make_html is False and registering == 0
            logger.info('registration - finish')

    def report_registered_assets(self):
        self.transform_model_instance['pdfs'] = []
        self.transform_model_instance['htmls'] = []
        
        for asset in self.assets_items:
            if asset.status() == 'registered':
                data = {}
                data['type'] = asset.filetype
                data['language'] = asset.metadata.get('lang')
                data['url'] = asset.url
                if asset.filetype == 'pdf':
                    self.transform_model_instance['pdfs'].append(data)
                elif asset.filetype == 'html':
                    self.transform_model_instance['htmls'].append(data)
                elif asset.filetype == 'xml':
                    self.transform_model_instance['xml'] = asset.url
            else:
                logger.error(asset.error_message)
