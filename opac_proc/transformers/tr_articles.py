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

        self.source_files = source_files_handler.SourceFiles(xylose_article, config.OPAC_PROC_CSS_PATH)

        self.queued_assets = {}
        ssm_status = assets_handler.client_status()
        if ssm_status is True:
            self.queued_assets['media'] = self.queue_media_registrations()
            self.queued_assets['xml'] = self.queue_xml_registrations()
            self.queued_assets['pdf'] = self.queue_pdf_registrations()
        

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

        self.transform_model_instance['assets'] = {}
        
        assets_sources = {}
        assets_sources['registration'] = ssm_status
        assets_sources['htmls'] = htmls
        assets_sources['pdfs'] = self.assets_sources_pdf()
        assets_sources['media'] = self.assets_sources_media()
        if self.source_files.xml_file is not None:
            assets_sources['xml'] = self.assets_sources_xml()
        self.transform_model_instance['assets']['sources'] = assets_sources

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
        registered_assets = {}
        if len(queued_assets) > 0:
            registered_assets['pdfs'] = self.registered_assets_pdf(queued_assets.get('pdf'))
            if self.source_files.xml_file is not None:
                registered_assets['xml'] = self.registered_assets_xml(queued_assets.get('xml'))
                queued_assets['htmls'] = self.queue_generated_html_registrations(self.registered_assets_media(queued_assets.get('media')))
                registered_assets['htmls'] = self.registered_assets_generated_html(queued_assets['htmls'])

            self.transform_model_instance['assets']['registered'] = registered_assets
            
            self.transform_model_instance['pdfs'] = []
            for lang, asset_data in registered_assets['pdfs'].items():
                self.transform_model_instance['pdfs'].append({'language': lang, 'type': 'pdf', 'url': asset_data.get('url')})

            if self.source_files.xml_file is not None:
                self.transform_model_instance['xml'] = registered_assets['xml'].get('url')
                self.transform_model_instance['htmls'] = []
                for lang, asset_data in registered_assets['htmls'].items():
                    self.transform_model_instance['htmls'].append({'language': lang, 'type': 'html', 'url': asset_data.get('url')})

        return self.transform_model_instance

    def assets_sources_pdf(self):
        assets_items = {}
        for lang, source_file in self.source_files.pdf_files.items():
            assets_items[lang] = source_file.source_location
        return assets_items

    def queue_pdf_registrations(self):
        assets_items = {}
        for lang, source_file in self.source_files.pdf_files.items():
            metadata = self.source_files.article_metadata.copy()
            metadata.update({'aid': self.article_uuid})
            metadata.update({'lang': lang})
            if source_file.location is not None:
                try:
                    pfile = open(source_file.location, 'rb')
                except Exception, e:
                    logger.error(u'Não foi possível abrir o arquivo {}'.format(source_file.source_location))
                    continue
                else:
                    asset = assets_handler.Asset(pfile, source_file.filename, 'pdf', metadata, self.source_files.bucket_name)
                    if asset.register():
                        assets_items[lang] = asset
        return assets_items

    def registered_assets_pdf(self, queued_assets_pdf):
        registered = {}
        queued = queued_assets_pdf.copy()
        while len(registered) < len(queued):
            for lang, asset in queued.items():
                if not lang in registered.keys() and asset.status == 'registered':
                    registered[lang] = asset.data
                    del queued[lang]
        return registered

    def assets_sources_media(self):
        assets_items = {}
        for href, source_file in self.source_files.media_files.items():
            assets_items[href.replace('.', '-DOT-')] = source_file.source_location
        return assets_items

    def queue_media_registrations(self):
        assets = {}
        for fname, source_file in self.source_files.media_files.items():
            metadata = self.source_files.article_metadata.copy()
            metadata.update({'filename': fname, 'name': source_file.name, 'ext': source_file.ext})
            metadata.update({'aid': self.article_uuid})
            
            if source_file.location is None:
                asset = {'error message': u'Não encontrado o arquivo {}'.format(source_file.source_location)}
            else:
                try:
                    pfile = open(source_file.location, 'rb')
                except Exception, e:
                    logger.error(u'Não foi possível abrir o arquivo {}'.format(source_file.source_location))
                    continue
                else:
                    asset = assets_handler.Asset(pfile, fname, '', metadata, self.source_files.bucket_name)
                    asset.register()
                    assets[fname] = asset
        return assets

    def registered_assets_media(self, queued_assets_media):
        registered = {}
        queued = queued_assets_media.copy()
        while len(registered) < len(queued):
            for fname, asset in queued.items():
                if not fname in registered.keys() and asset.status == 'registered':
                    label = fname.replace('.', '-DOT-')
                    registered[label] = asset.data
                    source_file = self.source_files.media_files.get(fname)
                    registered[label].update({'name': source_file.name, 'ext': source_file.ext})
                    del queued[fname]
        return registered

    def assets_sources_xml(self):
        if self.source_files.xml_file is not None:
            return self.source_files.xml_file.source_location

    def queue_xml_registrations(self):
        if self.source_files.xml_file is None:
            return None
        ret = None
        metadata = self.source_files.article_metadata.copy()
        metadata.update(file_metadata)
        metadata.update({'aid': self.article_uuid})
            
        source_file = self.source_files.xml_file
        if source_file.location is not None:
            try:
                pfile = open(source_file.location, 'rb')
            except Exception, e:
                logger.error(u'Não foi possível abrir o arquivo {}'.format(source_file.source_location))
            else:
                asset = assets_handler.Asset(pfile, source_file.name, 'xml', metadata, self.source_files.bucket_name)
                asset.register()
                ret = asset
        return ret
        
    def registered_assets_xml(self, queued_assets_xml):
        if queued_assets_xml is not None:
            while queued_assets_xml.status == 'queued':
                pass
            return queued_assets_xml.data if queued_assets_xml.status == 'registered' else None

    def queue_generated_html_registrations(self, queued_assets):
        assets = {}
        media = queued_assets.get('media')
        if self.source_files.generated_html is not None:
            if self.source_files.generated_html.get('generated htmls') is not None:
                for lang, content in self.source_files.generated_html.items():
                    if media is not None:
                        for media_name, media_data in media.items():
                            url = media_data.get('url')
                            if url is not None:
                                href_content = 'href="{}"'.format(media_name.replace('-DOT-', '.'))
                                ssm_href_content = 'href="{}"'.format(data.get('url'))
                                content = html.replace(href_content, ssm_href_content)
                    pfile = StringIO.StringIO(content.encode('utf-8'))
                            
                    filename = lang+'_'+self.source_files.article_folder_name + '.html'
                        
                    metadata = self.source_files.article_metadata.copy()
                    metadata.update({'aid': self.article_uuid})
                    
                    try:
                        asset = assets_handler.Asset(pfile, filename, 'html', metadata, self.source_files.bucket_name)
                    except Exception, e:
                        logger.error(u'Não foi possível ler o arquivo html gerado correspondente a {}'.format(filename))
                        continue
                    else:
                        asset.register()
                        assets[lang] = asset
        return assets
        
    def registered_assets_generated_html(self, queued_assets_generated_html):
        registered = {}
        queued = queued_assets_generated_html.copy()
        while len(registered) < len(queued):
            for lang, asset in queued.items():
                if not lang in registered.keys() and asset.status == 'registered':
                    registered[lang] = asset.data
                    del queued[lang]
        return registered
